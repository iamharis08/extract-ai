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
        '%d %B, %Y',  # 31 January, 2016
        '%m/%d/%Y',  # 01/31/2016
        '%m-%d-%Y',  # 01-31-2016
        '%Y/%m/%d',  # 2016/01/31
        '%Y-%m-%d',  # 2016-01-31
        '%d/%m/%Y',  # 14/08/2023 
        '%d-%m-%Y',  # 14-08-2023 
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



def clean_numeric_fields(item_dict, keys_to_clean):
    """
    A helper function that takes a dictionary and a set of keys,
    and safely converts the values for those keys to floats.
    """
    # We loop through a copy of the items to safely modify the dictionary.
    for key, value in list(item_dict.items()):
        # We only operate on the keys specified in our set.
        if key in keys_to_clean:
            # We use the same robust try/except block as before.
            try:
                # Remove commas from numbers like "1,187.79" before converting.
                item_dict[key] = float(str(value).replace(',', '')) if value else None
            except (ValueError, TypeError):
                # If conversion fails for any reason, set the value to None.
                item_dict[key] = None
    return item_dict



def process_invoice(event, context):
    """
    Cloud Function triggered by a Pub/Sub message when a new invoice is uploaded.
    It processes the document using Document AI and stores the result in BigQuery.
    """

    # 1. Decode the Pub/Sub message to get file details ---------------------/
    
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

    # 2. Function grabs the google cloud storage url path for uploaded invoice

    gcs_uri = f"gs://{bucket_name}/{full_gcs_path}"
    base_processor_name = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)
    processor_name = f"{base_processor_name}/processorVersions/stable"

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

    #----------------------------------------------------------------------------------------------
    #SECTION TRANSFORM, PARSE, AND CLEAN DATA
    # Transform, parse, and clean data returned from Invoice Parser AI for BIGQUERY insert

    raw_flat_data = {}
    parsed_line_items = []
    parsed_vat = []

    
    # EXAMPLE PROP ---------------------
    #{
    # type_: "line_item/quantity",
    # mention_text: "7",
    # confidence_level: 0.99,
    # properties: [......]
    # }
    # ----------------------------------
    
    # Loop through the nested properties (quantity, description, etc.) and make it into a list of items
    #formatting list_items and vat for BigQuery

    for entity in document.entities:
        key = entity.type_

        if key in ['line_item', 'vat']:
            nested_dict = {}
            for prop in entity.properties:
                prop_key = prop.type_.split('/')[-1]
                prop_value = prop.mention_text.replace('\n', ' ')
                nested_dict[prop_key] = prop_value
            
            if key == 'line_item':
                parsed_line_items.append(nested_dict)
            elif key == 'vat':
                parsed_vat.append(nested_dict)
            
        else:
            raw_flat_data[key] = entity.mention_text.replace('\n', ' ')

    row_to_insert['line_items'] = parsed_line_items
    row_to_insert['vat'] = parsed_vat

    # --------------------------------------------------------------------
    # The AI sometimes finds more fields than we need and so we specify all the 
    # columns we need to set for BigQuery so that no field is added thats not in BigQuery

    BQ_FLAT_COLUMNS = {
        'supplier_name', 'supplier_address', 'supplier_email', 'supplier_phone', 
        'supplier_website', 'supplier_tax_id', 'supplier_iban', 
        'supplier_payment_ref', 'supplier_registration', 'receiver_name', 
        'receiver_address', 'receiver_email', 'receiver_phone', 'receiver_website', 
        'receiver_tax_id', 'invoice_id', 'invoice_date', 'due_date', 
        'delivery_date', 'currency', 'currency_exchange_rate', 'net_amount', 
        'total_tax_amount', 'freight_amount', 'amount_paid_since_last_invoice', 
        'total_amount', 'purchase_order', 'payment_terms', 'carrier', 
        'ship_to_name', 'ship_to_address', 'ship_from_name', 
        'ship_from_address', 'remit_to_name', 'remit_to_address'
    }

    row_to_insert = {
        entity.type_: entity.mention_text.replace('\n', ' ')
        for entity in document.entities
        if entity.type_ in BQ_FLAT_COLUMNS
    }
    # -------------------------------------------------------------------

    # Clean and format date fields for BigQuery compatibility
    date_keys = ['invoice_date', 'due_date', 'delivery_date']
    for key in date_keys:
        if date_string := row_to_insert.get(key):
            row_to_insert[key] = robust_date_parser(date_string)

    # ------------------------------------------------------------------

    #cleaning data from list_items and vat properties for BigQuery Format
    line_item_numeric_keys = {'quantity', 'unit_price', 'amount'}
    vat_numeric_keys = {'tax_rate', 'tax_amount', 'amount', 'total_amount'}

  
    row_to_insert['line_items'] = [
        clean_numeric_fields(item, line_item_numeric_keys) 
        for item in row_to_insert.get('line_items', [])
    ]
    
    row_to_insert['vat'] = [
        clean_numeric_fields(item, vat_numeric_keys) 
        for item in row_to_insert.get('vat', [])
    ]

    # ---------------------------------------------------------------------------------------
    # 4. Add the final metadata fields that don't come from the AI.
    row_to_insert.update({
        'event_id': context.event_id,
        'source_gcs_path': gcs_uri,
        'user_id': user_id,
        'processed_timestamp': context.timestamp,
        'status': 'Pending_Approval'
    })
    
    print(f"Final prepared data for BigQuery: {row_to_insert}")

    # --- End Transform, Parse, and Clean --------------------------------------
    
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
    
