import os
import base64
import json
from dotenv import load_dotenv
from google.cloud import documentai_v1 as documentai
from google.cloud import bigquery
from google.cloud import pubsub_v1

# Load environment variables for local development
load_dotenv()

# --- CONFIGURATION ---
PROJECT_ID = os.getenv("PROJECT_ID") 
LOCATION = os.getenv("LOCATION")
PROCESSOR_ID = os.getenv("PROCESSOR_ID")

# --- Clients are initialized globally for reuse ---
docai_client = documentai.DocumentProcessorServiceClient()
bq_client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()


def process_invoice(event, context):
    """
    Cloud Function triggered by a Pub/Sub message when a new invoice is uploaded.
    It processes the document using Document AI and stores the result in BigQuery.
    """
    # Decode the Pub/Sub message to get file details
    pubsub_message_data = base64.b64decode(event['data']).decode('utf-8')
    file_data = json.loads(pubsub_message_data)
    
    # Parse user_id and filename from the GCS path
    full_path = file_data['name']
    bucket_name = file_data['bucket']
    parts = full_path.split('/')

    if len(parts) != 2:
        print(f"Error: Invalid file path format: {full_path}")
        return

    user_id, filename = parts
    print(f"Processing '{filename}' for user '{user_id}'.")

    # Call the Document AI API to process the invoice
    gcs_uri = f"gs://{bucket_name}/{full_path}"
    processor_name = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)
    
    raw_document = documentai.RawDocument(gcs_uri=gcs_uri, mime_type="application/pdf")
    request = documentai.ProcessRequest(name=processor_name, raw_document=raw_document)
    
    result = docai_client.process_document(request=request)
    document = result.document

    # Parse the AI's response into a clean dictionary
    extracted_data = {
        entity.type_: entity.mention_text.replace('\n', '')
        for entity in document.entities
    }
    print(f"Successfully extracted data: {extracted_data}")

    

    # Prepare a clean row for BigQuery, defaulting missing fields to None
    # This prevents schema mismatch errors if the AI doesn't find a specific field.
    expected_keys = [
        'vendor_name', 'invoice_date', 'due_date', 
        'total_amount', 'tax_amount'
    ]
    row_to_insert = {key: extracted_data.get(key) for key in expected_keys}

    # Add additional metadata to the row
    row_to_insert.update({
        'invoice_id': context.event_id,
        'source_gcs_path': gcs_uri,
        'user_id': user_id,
        'processed_timestamp': context.timestamp,
        'status': 'Pending_Approval'
    })

    # Insert the row into the BigQuery table
    table_id = f"{PROJECT_ID}.invoice_processing.processed_invoices"
    try:
        errors = bq_client.insert_rows_json(table_id, [row_to_insert])
        if not errors:
            print("Successfully inserted row into BigQuery.")
        else:
            print(f"BigQuery insertion errors: {errors}")
    except Exception as e:
        print(f"Error inserting into BigQuery: {e}")