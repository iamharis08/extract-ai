import os
import base64
import json
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import documentai, bigquery, storage
import functions_framework

# --- CONFIGURATION & CLIENTS ---
load_dotenv()

class Settings:
    PROJECT_ID = os.getenv("PROJECT_ID")
    LOCATION = os.getenv("LOCATION") # For Document AI processor
    PROCESSOR_ID = os.getenv("PROCESSOR_ID")
    BQ_DATASET_ID = "invoice_processing"
    BQ_TABLE_ID = "processed_invoices"
    
SETTINGS = Settings()

docai_client = documentai.DocumentProcessorServiceClient()
bq_client = bigquery.Client(project=SETTINGS.PROJECT_ID)
storage_client = storage.Client(project=SETTINGS.PROJECT_ID)

# --- HELPER FUNCTIONS (Single Responsibility Principle) ---

def robust_date_parser(date_string: str | None) -> str | None:
    """Safely parses a date string from common formats into YYYY-MM-DD."""
    if not date_string: return None
    COMMON_DATE_FORMATS = ['%B %d, %Y', '%m/%d/%Y', '%m-%d-%Y', '%Y-%m-%d', '%d/%m/%Y']
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

def parse_pubsub_message(event_data: dict) -> dict:
    """Decodes the Pub/Sub message from the CloudEvent data payload."""
    if "message" not in event_data or "data" not in event_data["message"]:
        raise ValueError("Invalid Pub/Sub message format: 'message' or 'data' key missing.")

    pubsub_message = event_data["message"]
    message_data_decoded = base64.b64decode(pubsub_message["data"]).decode("utf-8")
    file_data = json.loads(message_data_decoded)
    
    full_gcs_path = file_data['name']
    bucket_name = file_data['bucket']
    
    parts = full_gcs_path.split('/')
    if len(parts) != 2:
        raise ValueError(f"Invalid file path format: {full_gcs_path}")

    user_id, filename = parts
    
    return {
        "user_id": user_id, "filename": filename,
        "gcs_uri": f"gs://{bucket_name}/{full_gcs_path}",
        "bucket_name": bucket_name, "full_gcs_path": full_gcs_path,
        "message_id": pubsub_message.get("messageId", "unknown-id"),
        "publish_time": pubsub_message.get("publishTime", datetime.utcnow().isoformat())
    }

def process_document_with_ai(gcs_uri: str) -> documentai.Document:
    """Calls the Document AI API and returns the processed document object."""
    base_processor_name = docai_client.processor_path(SETTINGS.PROJECT_ID, SETTINGS.LOCATION, SETTINGS.PROCESSOR_ID)
    processor_name = f"{base_processor_name}/processorVersions/stable"
    
    gcs_document = documentai.GcsDocument(gcs_uri=gcs_uri, mime_type="application/pdf")
    request = documentai.ProcessRequest(name=processor_name, gcs_document=gcs_document)
    
    print(f"Making request to Document AI processor for: {gcs_uri}")
    result = docai_client.process_document(request=request)
    return result.document

def transform_ai_response(document: documentai.Document) -> dict:
    """Transforms the raw AI response into a clean, typed dictionary."""
    final_row = {}
    parsed_line_items = []
    parsed_vat = []

    for entity in document.entities:
        key = entity.type_
        if key == 'line_item':
            nested_dict = {p.type_.split('/')[-1]: p.mention_text.replace('\n', ' ') for p in entity.properties}
            parsed_line_items.append(nested_dict)
        elif key == 'vat':
            nested_dict = {p.type_.split('/')[-1]: p.mention_text.replace('\n', ' ') for p in entity.properties}
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
    
    return final_row

def load_to_bigquery(row_to_insert: dict) -> None:
    """Loads a dictionary row into the destination BigQuery table."""
    table_id = f"{SETTINGS.PROJECT_ID}.{SETTINGS.BQ_DATASET_ID}.{SETTINGS.BQ_TABLE_ID}"
    errors = bq_client.insert_rows_json(table_id, [row_to_insert])
    if errors:
        raise RuntimeError(f"BigQuery insertion failed: {errors}")
    print("Successfully inserted row into BigQuery.")

def archive_file(bucket_name: str, full_gcs_path: str) -> None:
    """Moves a file from the raw bucket to the processed bucket."""
    source_bucket = storage_client.bucket(bucket_name)
    processed_bucket_name = f"{bucket_name.replace('-raw', '')}-processed"
    destination_bucket = storage_client.bucket(processed_bucket_name)
    source_blob = source_bucket.blob(full_gcs_path)
    
    source_bucket.copy_blob(source_blob, destination_bucket, full_gcs_path)
    source_blob.delete()
    print(f"Archived file '{full_gcs_path}'.")

# --- MAIN FUNCTION (The Orchestrator) ---

@functions_framework.cloud_event
def process_invoice(cloud_event):
    """
    The main entry point. Orchestrates the entire invoice processing pipeline.
    This is triggered by a CloudEvent from Pub/Sub.
    """
    try:
        # 1. Extract file info from the trigger event payload.
        #    The actual message is in the 'data' attribute of the CloudEvent.
        file_info = parse_pubsub_message(cloud_event.data)
        
        # 2. Process the document with the AI.
        document = process_document_with_ai(file_info["gcs_uri"])
        
        # 3. Transform the AI response into a clean data record.
        final_row = transform_ai_response(document)
        
        # 4. Add our application's metadata to the final record.
        final_row.update({
            'event_id': file_info["message_id"],
            'source_gcs_path': file_info["gcs_uri"],
            'user_id': file_info["user_id"],
            'processed_timestamp': file_info["publish_time"],
            'status': 'Pending_Approval'
        })
        
        print(f"Final prepared data for BigQuery: {final_row}")
        
        # 5. Load the clean record into our data warehouse.
        load_to_bigquery(final_row)
        
        # 6. Archive the original file now that the process is complete.
        archive_file(file_info["bucket_name"], file_info["full_gcs_path"])
        
    except Exception as e:
        print(f"FATAL: Error processing invoice: {e}")
        # Re-raising the exception is important for Cloud Functions error reporting.
        raise