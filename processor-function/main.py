import os
import base64
import json
from datetime import datetime
from dotenv import load_dotenv
import functions_framework

from google.cloud import documentai, bigquery, storage

# --- CONFIGURATION & CLIENTS ---
load_dotenv()

class Settings:
    PROJECT_ID = os.getenv("PROJECT_ID")
    LOCATION = os.getenv("LOCATION") # Processor location, e.g., "us"
    PROCESSOR_ID = os.getenv("PROCESSOR_ID")
    BQ_DATASET_ID = "invoice_processing"
    BQ_TABLE_ID = "processed_invoices"
    PROCESSED_BUCKET_SUFFIX = "-processed"

SETTINGS = Settings()

docai_client = documentai.DocumentProcessorServiceClient()
bq_client = bigquery.Client(project=SETTINGS.PROJECT_ID)
storage_client = storage.Client(project=SETTINGS.PROJECT_ID)


# --- HELPER FUNCTIONS (Single Responsibility Principle) ---

def robust_date_parser(date_string: str | None) -> str | None:
    """Safely parses a date string from common formats into YYYY-MM-DD."""
    if not date_string: return None
    COMMON_DATE_FORMATS = [
        '%B %d, %Y', '%m/%d/%Y', '%m-%d-%Y', '%Y-%m-%d', '%d/%m/%Y', '%d %B, %Y']
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

def parse_event_data(cloud_event) -> dict:
    """
    Decodes the Pub/Sub message from the full CloudEvent object.
    """
    # 1. Access the attributes object using the correct method you found.
    attributes = cloud_event.get_attributes()
    
    # 2. Access the data payload, which is a dictionary.
    event_data_payload = cloud_event.data

    if "message" not in event_data_payload or "data" not in event_data_payload["message"]:
        raise ValueError("Invalid CloudEvent payload: 'message' or nested 'data' key missing.")

    pubsub_message = event_data_payload["message"]
    
    # The actual data about the file is base64-encoded.
    message_data_encoded = pubsub_message["data"]
    message_data_decoded = base64.b64decode(message_data_encoded).decode("utf-8")
    file_data = json.loads(message_data_decoded)
    
    full_gcs_path = file_data.get('name')
    bucket_name = file_data.get('bucket')
    
    if not full_gcs_path or not bucket_name:
        raise ValueError("Decoded message is missing 'name' or 'bucket' key.")
        
    parts = full_gcs_path.split('/')
    if len(parts) != 2:
        raise ValueError(f"Invalid file path format: {full_gcs_path}")

    user_id, filename = parts
    
    # 3. Use the .get() method on the attributes object to safely get the metadata.
    return {
        "user_id": user_id,
        "filename": filename,
        "gcs_uri": f"gs://{bucket_name}/{full_gcs_path}",
        "bucket_name": bucket_name,
        "full_gcs_path": full_gcs_path,
        "event_id": attributes.get("id", "unknown-id"),
        "timestamp": attributes.get("time", datetime.utcnow().isoformat())
    }

def process_document_with_ai(gcs_uri: str) -> documentai.Document:
    """Calls the Document AI API and returns the processed document object."""
    processor_path = docai_client.processor_path(SETTINGS.PROJECT_ID, SETTINGS.LOCATION, SETTINGS.PROCESSOR_ID)
    request = documentai.ProcessRequest(
        name=f"{processor_path}/processorVersions/stable",
        gcs_document=documentai.GcsDocument(gcs_uri=gcs_uri, mime_type="application/pdf")
    )
    print(f"Making request to Document AI processor for: {gcs_uri}")
    result = docai_client.process_document(request=request)
    return result.document

def transform_ai_response(document: documentai.Document) -> dict:
    """
    Transforms the raw AI response into a clean, typed dictionary that matches our schema.
    """
    row_to_insert = {}
    parsed_line_items = []
    parsed_vat = []
    
    # We loop through the AI's entity list only ONCE.
    for entity in document.entities:
        key = entity.type_
        
        # Check if the entity is a structured line_item
        if key == 'line_item':
            nested_dict = {p.type_.split('/')[-1]: p.mention_text.replace('\n', ' ') for p in entity.properties}
            parsed_line_items.append(nested_dict)
            
        # Check if the entity is a structured vat/tax item
        elif key == 'vat':
            nested_dict = {p.type_.split('/')[-1]: p.mention_text.replace('\n', ' ') for p in entity.properties}
            parsed_vat.append(nested_dict)
            
        # Otherwise, it's a simple, flat entity.
        else:
            row_to_insert[key] = entity.mention_text.replace('\n', ' ')

    # Now, add the correctly parsed nested lists to our dictionary
    row_to_insert['line_items'] = parsed_line_items
    row_to_insert['vat'] = parsed_vat

    # Clean and format date fields
    date_keys = ['invoice_date', 'due_date', 'delivery_date']
    for key in date_keys:
        if key in row_to_insert:
            row_to_insert[key] = robust_date_parser(row_to_insert[key])

    # Clean and format numeric fields using our helper function
    top_level_numeric_keys = {'net_amount', 'total_tax_amount', 'total_amount', 'freight_amount'}
    row_to_insert = clean_numeric_fields(row_to_insert, top_level_numeric_keys)
    
    line_item_numeric_keys = {'quantity', 'unit_price', 'amount'}
    row_to_insert['line_items'] = [clean_numeric_fields(item, line_item_numeric_keys) for item in row_to_insert.get('line_items', [])]
    
    vat_numeric_keys = {'tax_rate', 'tax_amount', 'amount', 'total_amount'}
    row_to_insert['vat'] = [clean_numeric_fields(item, vat_numeric_keys) for item in row_to_insert.get('vat', [])]
    
    return row_to_insert

def load_to_bigquery(row_to_load: dict, schema: set) -> None:
    """Loads a dictionary row into the destination BigQuery table, filtering for schema."""
    table_id = f"{SETTINGS.PROJECT_ID}.{SETTINGS.BQ_DATASET_ID}.{SETTINGS.BQ_TABLE_ID}"
    
    # Filter the final row to only include columns that exist in our BQ schema
    filtered_row = {k: v for k, v in row_to_load.items() if k in schema}

    errors = bq_client.insert_rows_json(table_id, [filtered_row])
    if errors:
        raise RuntimeError(f"BigQuery insertion failed: {errors}")
    print("Successfully inserted row into BigQuery.")

def archive_file(bucket_name: str, full_gcs_path: str) -> None:
    """Moves a file from the raw bucket to the processed bucket."""
    source_bucket = storage_client.bucket(bucket_name)
    destination_bucket_name = f"{bucket_name.replace('-raw', '')}-processed" # Uses suffix from Settings
    destination_bucket = storage_client.bucket(destination_bucket_name)
    source_blob = source_bucket.blob(full_gcs_path)
    
    source_bucket.copy_blob(source_blob, destination_bucket, full_gcs_path)
    source_blob.delete()
    print(f"Archived file '{full_gcs_path}'.")

# --- MAIN ORCHESTRATOR ---

@functions_framework.cloud_event
def process_invoice(cloud_event):
    """Orchestrates the entire invoice processing pipeline."""
    print(cloud_event, "eventtt")
    print(cloud_event.get_attributes(), "DATA")
    try:
        file_info = parse_event_data(cloud_event)
        document = process_document_with_ai(file_info["gcs_uri"])
        transformed_row = transform_ai_response(document)
        
        transformed_row.update({
            'event_id': file_info["event_id"],
            'source_gcs_path': file_info["gcs_uri"],
            'user_id': file_info["user_id"],
            'processed_timestamp': file_info["timestamp"],
            'status': 'Pending_Approval'
        })
        
        # Get the definitive schema from the live BigQuery table
        table_id = f"{SETTINGS.PROJECT_ID}.{SETTINGS.BQ_DATASET_ID}.{SETTINGS.BQ_TABLE_ID}"
        table = bq_client.get_table(table_id)
        schema_keys = {field.name for field in table.schema}
        
        # Load the clean record into our data warehouse
        load_to_bigquery(transformed_row, schema_keys)
        
        # Archive the original file now that the process is complete
        archive_file(file_info["bucket_name"], file_info["full_gcs_path"])
        
    except Exception as e:
        print(f"FATAL: Error processing invoice: {e}")
        return "Event ignored", 200
        