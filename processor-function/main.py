import os
import base64
import json
from datetime import datetime
from dotenv import load_dotenv
from flask import Flask, request
from google.cloud import documentai, bigquery, storage

# --- 1. Centralized Configuration ---
load_dotenv()

class Settings:
    PROJECT_ID = os.getenv("PROJECT_ID")
    DOCAI_LOCATION = os.getenv("DOCAI_LOCATION")
    PROCESSOR_ID = os.getenv("PROCESSOR_ID")
    BQ_DATASET_ID = "invoice_processing"
    BQ_TABLE_ID = "processed_invoices"
    RAW_BUCKET_SUFFIX = "-raw"
    PROCESSED_BUCKET_SUFFIX = "-processed"

SETTINGS = Settings()

# --- 2. Client Initialization ---
docai_client = documentai.DocumentProcessorServiceClient()
bq_client = bigquery.Client(project=SETTINGS.PROJECT_ID)
storage_client = storage.Client(project=SETTINGS.PROJECT_ID)

# --- 3. Helper Functions (Single Responsibility) ---

def robust_date_parser(date_string: str | None) -> str | None:
    """Safely parses a date string from common formats into YYYY-MM-DD."""
    if not date_string: return None
    COMMON_DATE_FORMATS = ['%B %d, %Y', '%m/%d/%Y', '%m-%d-%Y', '%Y-%m-%d', '%d/%m/%Y', '%d %B, %Y']
    for fmt in COMMON_DATE_FORMATS:
        try:
            return datetime.strptime(date_string, fmt).strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            continue
    print(f"Warning: Could not parse date '{date_string}' with any known format.")
    return None

def clean_numeric_fields(item_dict: dict, keys_to_clean: set) -> dict:
    """Safely converts string values in a dictionary to floats, removing commas."""
    for key, value in list(item_dict.items()):
        if key in keys_to_clean:
            try:
                item_dict[key] = float(str(value).replace(',', '')) if value else None
            except (ValueError, TypeError):
                item_dict[key] = None
    return item_dict

def parse_pubsub_message(envelope: dict) -> dict:
    """Decodes the Pub/Sub message from an HTTP request and extracts file details."""
    if not envelope or "message" not in envelope:
        raise ValueError("Invalid Pub/Sub message format: missing 'message' field.")
    
    pubsub_message = envelope["message"]
    message_data = base64.b64decode(pubsub_message["data"]).decode('utf-8')
    file_data = json.loads(message_data)
    
    full_path = file_data.get("name")
    bucket_name = file_data.get("bucket")
    
    if not all([full_path, bucket_name]):
        raise ValueError("Invalid GCS event data: missing 'name' or 'bucket'.")
        
    parts = full_path.split('/')
    if len(parts) != 2:
        raise ValueError(f"Invalid file path format: {full_path}")
        
    user_id, filename = parts
    return {
        "user_id": user_id, "filename": filename,
        "gcs_uri": f"gs://{bucket_name}/{full_path}",
        "bucket_name": bucket_name, "full_path": full_path,
        "event_id": pubsub_message.get("messageId", "no-message-id"),
        "timestamp": pubsub_message.get("publishTime", datetime.utcnow().isoformat())
    }

def process_document_with_ai(gcs_uri: str) -> documentai.Document:
    """Calls the Document AI API and returns the processed document object."""
    base_processor_name = docai_client.processor_path(SETTINGS.PROJECT_ID, SETTINGS.DOCAI_LOCATION, SETTINGS.PROCESSOR_ID)
    processor_name = f"{base_processor_name}/processorVersions/stable"
    
    gcs_document = documentai.GcsDocument(gcs_uri=gcs_uri, mime_type="application/pdf")
    request_docai = documentai.ProcessRequest(name=processor_name, gcs_document=gcs_document)
    
    print(f"Making request to Document AI processor for: {gcs_uri}")
    result = docai_client.process_document(request=request_docai)
    return result.document

def transform_ai_response(document: documentai.Document) -> dict:
    """Transforms the raw AI response into a clean, typed dictionary for BigQuery."""
    final_row = {}
    parsed_line_items = []
    parsed_vat = []

    for entity in document.entities:
        key = entity.type_
        if key == 'line_item':
            nested_dict = {prop.type_.split('/')[-1]: prop.mention_text.replace('\n', ' ') for prop in entity.properties}
            parsed_line_items.append(nested_dict)
        elif key == 'vat':
            nested_dict = {prop.type_.split('/')[-1]: prop.mention_text.replace('\n', ' ') for prop in entity.properties}
            parsed_vat.append(nested_dict)
        else:
            final_row[key] = entity.mention_text.replace('\n', ' ')

    final_row['line_items'] = parsed_line_items
    final_row['vat'] = parsed_vat

    date_keys = ['invoice_date', 'due_date', 'delivery_date']
    for key in date_keys:
        if key in final_row:
            final_row[key] = robust_date_parser(final_row[key])

    top_level_numeric_keys = {'net_amount', 'total_tax_amount', 'total_amount'}
    final_row = clean_numeric_fields(final_row, top_level_numeric_keys)

    line_item_numeric_keys = {'quantity', 'unit_price', 'amount'}
    final_row['line_items'] = [clean_numeric_fields(item, line_item_numeric_keys) for item in final_row.get('line_items', [])]
    
    vat_numeric_keys = {'tax_rate', 'tax_amount', 'amount', 'total_amount'}
    final_row['vat'] = [clean_numeric_fields(item, vat_numeric_keys) for item in final_row.get('vat', [])]
    
    return final_row

def load_to_bigquery(row_to_insert: dict) -> None:
    """Loads a dictionary row into the destination BigQuery table."""
    table_id = f"{SETTINGS.PROJECT_ID}.{SETTINGS.BQ_DATASET_ID}.{SETTINGS.BQ_TABLE_ID}"
    errors = bq_client.insert_rows_json(table_id, [row_to_insert])
    if errors:
        print(f"BigQuery insertion errors: {errors}")
        raise RuntimeError(f"BigQuery insertion failed: {errors}")
    print("Successfully inserted row into BigQuery.")

def archive_file(bucket_name: str, full_path: str) -> None:
    """Moves a file from the raw bucket to the processed bucket."""
    source_bucket = storage_client.bucket(bucket_name)
    # Note: A more robust implementation would get the processed bucket name from settings
    destination_bucket = storage_client.bucket(f"{bucket_name.replace('-raw', '')}-processed")
    source_blob = source_bucket.blob(full_path)
    
    source_bucket.copy_blob(source_blob, destination_bucket, full_path)
    source_blob.delete()
    print(f"Archived file '{full_path}'.")

# --- MAIN APPLICATION (The Orchestrator) ---
app = Flask(__name__)

@app.route("/", methods=["POST"])
def process_invoice_http():
    """
    The main HTTP endpoint. It orchestrates the entire invoice processing pipeline.
    """
    try:
        # 1. Extract file info from the trigger event
        file_info = parse_pubsub_message(request.get_json())
        
        # 2. Process the document with the AI
        document = process_document_with_ai(file_info["gcs_uri"])
        
        # 3. Transform the AI response into a clean data record
        final_row = transform_ai_response(document)
        
        # 4. Add our own application metadata
        final_row.update({
            'event_id': file_info["event_id"],
            'source_gcs_path': file_info["gcs_uri"],
            'user_id': file_info["user_id"],
            'processed_timestamp': file_info["timestamp"],
            'status': 'Pending_Approval'
        })
        
        # 5. Load the clean record into our data warehouse
        load_to_bigquery(final_row)
        
        # 6. Archive the original file if the load was successful
        archive_file(file_info["bucket_name"], file_info["full_path"])

        return ("OK", 204) # Success, no content
        
    except Exception as e:
        print(f"FATAL: Error processing invoice: {e}")
        # Return an error status code to tell Pub/Sub to retry the message
        return (f"Internal Server Error: {e}", 500)