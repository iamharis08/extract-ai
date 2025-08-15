import os
import base64
import json
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import documentai_v1 as documentai
from google.cloud import bigquery
from google.cloud import pubsub_v1
from google.cloud import storage

# Load environment variables for local development
load_dotenv()

# --- CONFIGURATION ---
PROJECT_ID = os.getenv("PROJECT_ID") 
LOCATION = os.getenv("LOCATION")
# AI_LOCATION = os.getenv("AI_LOCATION")
PROCESSOR_ID = os.getenv("PROCESSOR_ID")

# --- Clients are initialized globally for reuse ---
docai_client = documentai.DocumentProcessorServiceClient()
bq_client = bigquery.Client(project=PROJECT_ID)
publisher = pubsub_v1.PublisherClient()
storage_client = storage.Client(project=PROJECT_ID)




def robust_date_parser(date_string):
    """
    Tries to parse a date string using a list of common formats.
    Returns the date in 'YYYY-MM-DD' format if successful, otherwise None.
    """
    #we list the common date formats we expect to see in the invoices
    COMMON_DATE_FORMATS = [
        '%B %d, %Y',  # January 31, 2016
        '%m/%d/%Y',  # 01/31/2016
        '%m-%d-%Y',  # 01-31-2016
        '%Y-%m-%d',  # 2016-01-31
        '%d-%m-%Y',  # 14/08/2023 
    ]

    # We loop through each format in our list.
    for fmt in COMMON_DATE_FORMATS:
        try:
            # We attempt to parse the date_string using the current format.
            # If it succeeds, we format it correctly for BigQuery and return it.
            date_object = datetime.strptime(date_string, fmt)
            return date_object.strftime('%Y-%m-%d')
        except ValueError:
            # If a ValueError occurs, it means the format didn't match.
            # We do nothing and just let the loop continue to the next format.
            continue
    
    # If the loop finishes and no formats have matched, the date is unknown.
    # We return None to indicate failure.
    print(f"Warning: Could not parse date '{date_string}' with any known format.")
    return None





def process_invoice(event, context):
    """
    Cloud Function triggered by a Pub/Sub message when a new invoice is uploaded.
    It processes the document using Document AI and stores the result in BigQuery.
    """

    # This section is for the pub/sub and exctracting data from it ---------------------/
    # Decode the Pub/Sub message to get file details
    pubsub_message_data = base64.b64decode(event['data']).decode('utf-8')
    file_data = json.loads(pubsub_message_data)
    
    # Parse user_id and filename from the GCS path
    full_gcs_path = file_data['name']
    bucket_name = file_data['bucket']
    parts = full_gcs_path.split('/')

    if len(parts) != 2:
        print(f"Error: Invalid file path format: {full_gcs_path}")
        return

    user_id, filename = parts
    print(f"Processing '{filename}' for user '{user_id}'.")
   #----------------------------------------------------------------------------
    #Function
    #grabs the google cloud storage url path for uploaded invoice
    gcs_uri = f"gs://{bucket_name}/{full_gcs_path}"
    processor_name = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)
    
     # then calls the Document AI API to process the invoice
    gcs_document = documentai.GcsDocument(
        gcs_uri=gcs_uri, 
        mime_type="application/pdf"
        )
    request = documentai.ProcessRequest(
        name=processor_name,
        gcs_document=gcs_document
        )
    
    result = docai_client.process_document(request=request)
    document = result.document

    #--------------------------------------------------------------------------
    # --- Parse, Transform, and Finalize Data for BigQuery ---
    
   # Parse flat entities directly into a dictionary
    row_to_insert = {
        entity.type_: entity.mention_text.replace('\n', ' ')
        for entity in document.entities
    }

    # Handle the line_item field to match the BigQuery ARRAY<STRUCT> schema
    if 'line_item' in row_to_insert:
        # Convert the single text entity into a list containing one structured item
        row_to_insert['line_items'] = [{
            'description': row_to_insert['line_item'], 
            'quantity': None, 'unit_price': None, 'amount': None
        }]
        del row_to_insert['line_item']
    else:
        row_to_insert['line_items'] = []

    # Clean and format date fields
    for key in ['invoice_date', 'due_date']:
        if date_string := row_to_insert.get(key):
            row_to_insert[key] = robust_date_parser(date_string)

    print(f"Final prepared data for BigQuery: {row_to_insert}")

    # 4. Add the final metadata fields that don't come from the AI.
    row_to_insert.update({
        'event_id': context.event_id,
        'source_gcs_path': gcs_uri,
        'user_id': user_id,
        'processed_timestamp': context.timestamp,
        'status': 'Pending_Approval'
    })
    
    print(f"Final prepared data for BigQuery: {row_to_insert}")
    # --- End Transformation ---

    # Insert the row into the BigQuery table
    table_id = f"{PROJECT_ID}.invoice_processing.processed_invoices"

    try:
        errors = bq_client.insert_rows_json(table_id, [row_to_insert])
        if not errors:
            
            print("Successfully inserted row into BigQuery.")

            #we are now copying the raw file that was just processed succesfully, deleting from raw bucket, then adding it to processed bucket
            source_bucket = storage_client.bucket(bucket_name)
            destination_bucket = storage_client.bucket(f"{bucket_name.replace('-raw', '')}-processed")
            source_blob = source_bucket.blob(full_gcs_path)

            # Copy to the processed bucket and then delete from the raw bucket.
            source_bucket.copy_blob(
                source_blob, 
                destination_bucket, 
                full_gcs_path)
            source_blob.delete()
            
            print(f"Archived file to processed bucket.")

        else:
            print(f"BigQuery insertion errors: {errors}")
    except Exception as e:
        print(f"Error inserting into BigQuery: {e}")
    
