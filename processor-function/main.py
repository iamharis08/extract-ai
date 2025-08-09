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

# --- CLIENTS (Initialized globally for reuse) ---
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