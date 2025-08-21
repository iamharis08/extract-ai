import os
import base64
import json
from datetime import datetime
from dotenv import load_dotenv
from google.cloud import documentai_v1 as documentai
from google.cloud import bigquery
from google.cloud import storage
import functions_framework

# --- CONFIGURATION & CLIENTS ---
# This loads variables from your .env file for local development.
load_dotenv()

# Centralizing configuration in a class makes it explicit and easy to manage.
class Settings:
    PROJECT_ID = os.getenv("PROJECT_ID") 
    LOCATION = os.getenv("LOCATION") # General region for services
    PROCESSOR_ID = os.getenv("PROCESSOR_ID")
    BQ_DATASET_ID = "invoice_processing"
    BQ_TABLE_ID = "processed_invoices"
    PROCESSED_BUCKET_SUFFIX = "-processed"

SETTINGS = Settings()

# Clients are initialized once in the global scope for efficiency.
# The function won't have to recreate them on every invocation.
docai_client = documentai.DocumentProcessorServiceClient()
bq_client = bigquery.Client(project=SETTINGS.PROJECT_ID)
storage_client = storage.Client(project=SETTINGS.PROJECT_ID)


# --- HELPER FUNCTIONS (Single Responsibility Principle) ---

def robust_date_parser(date_string: str | None) -> str | None:
    """
    Tries to parse a date string using a list of common formats.
    Returns the date in 'YYYY-MM-DD' format if successful, otherwise None.
    """
    # We list the common date formats we expect to see in the invoices.
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
    if not date_string:
        return None
        
    for fmt in COMMON_DATE_FORMATS:
        try:
            # We attempt to parse the date_string using the current format.
            date_object = datetime.strptime(date_string, fmt)
            # If it succeeds, we format it correctly for BigQuery and return it.
            return date_object.strftime('%Y-%m-%d')
        except (ValueError, TypeError):
            # If an error occurs, the format didn't match. We continue to the next.
            continue
    
    print(f"Warning: Could not parse date '{date_string}' with any known format.")
    return None

def clean_numeric_fields(item_dict: dict, keys_to_clean: set) -> dict:
    """
    A helper function that takes a dictionary and a set of keys,
    and safely converts the values for those keys to floats, removing commas.
    """
    for key, value in list(item_dict.items()):
        if key in keys_to_clean:
            try:
                # It takes the value, converts it to a string just in case,
                # REMOVES any commas, and then converts to a float.
                clean_value_str = str(value).replace(',', '')
                item_dict[key] = float(clean_value_str) if value else None
            except (ValueError, TypeError):
                # If conversion fails for any other reason, set to None.
                item_dict[key] = None
    return item_dict

def parse_pubsub_message(cloud_event: dict) -> dict:
    """
    Its only job is to decode the Pub/Sub message from the CloudEvent
    and extract the file and user details.
    """
    # The actual message data is nested and base64-encoded.
    message_data_encoded = cloud_event.data["message"]["data"]
    message_data_decoded = base64.b64decode(message_data_encoded).decode("utf-8")
    file_data = json.loads(message_data_decoded)
    
    full_gcs_path = file_data['name']
    bucket_name = file_data['bucket']
    
    # We parse the user's ID and the filename from the file's path.
    parts = full_gcs_path.split('/')
    if len(parts) != 2:
        raise ValueError(f"Invalid file path format: {full_gcs_path}")

    user_id, filename = parts
    
    # It returns a clean dictionary with all the info needed by other functions.
    return {
        "user_id": user_id,
        "filename": filename,
        "gcs_uri": f"gs://{bucket_name}/{full_gcs_path}",
        "bucket_name": bucket_name,
        "full_gcs_path": full_gcs_path,
        "event_id": cloud_event.id,
        "timestamp": cloud_event.time
    }

def process_document_with_ai(gcs_uri: str) -> documentai.Document:
    """
    Its only job is to call the Document AI API with the file's location
    and return the resulting 'document' object.
    """
    base_processor_name = docai_client.processor_path(SETTINGS.PROJECT_ID, SETTINGS.LOCATION, SETTINGS.PROCESSOR_ID)
    processor_name = f"{base_processor_name}/processorVersions/stable"

    gcs_document = documentai.GcsDocument(gcs_uri=gcs_uri, mime_type="application/pdf")
    request = documentai.ProcessRequest(name=processor_name, gcs_document=gcs_document)
    
    print(f"Making request to Document AI processor for: {gcs_uri}")
    result = docai_client.process_document(request=request)
    return result.document

def transform_ai_response(document: documentai.Document) -> dict:
    """
    Its only job is to transform the raw, complex AI response into a clean,
    flat dictionary with the correct data types, ready for BigQuery.
    """
    # Define the columns we want to keep. This is our schema contract.
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

    # Prepare containers for our different data types
    row_to_insert = {}
    parsed_line_items = []
    parsed_vat = []
    
    # We loop through the AI's entity list only ONCE.
    for entity in document.entities:
        key = entity.type_
        
        # Check if the entity is a structured line_item
        if key == 'line_item':
            nested_dict = {}
            for prop in entity.properties:
                prop_key = prop.type_.split('/')[-1]
                nested_dict[prop_key] = prop.mention_text.replace('\n', ' ')
            parsed_line_items.append(nested_dict)
            
        # Check if the entity is a structured vat/tax item
        elif key == 'vat':
            nested_dict = {}
            for prop in entity.properties:
                prop_key = prop.type_.split('/')[-1]
                nested_dict[prop_key] = prop.mention_text.replace('\n', ' ')
            parsed_vat.append(nested_dict)
            
        # For all other entities, check if they are in our whitelist of flat columns.
        elif key in BQ_FLAT_COLUMNS:
            row_to_insert[key] = entity.mention_text.replace('\n', ' ')

    # Now, add the correctly parsed nested lists to our final dictionary
    row_to_insert['line_items'] = parsed_line_items
    row_to_insert['vat'] = parsed_vat

    # --- The rest of the cleaning and casting logic remains the same ---

    # Clean and format date fields.
    date_keys = ['invoice_date', 'due_date', 'delivery_date']
    for key in date_keys:
        if key in row_to_insert:
            row_to_insert[key] = robust_date_parser(row_to_insert[key])

    # Clean and format numeric fields.
    top_level_numeric_keys = {'net_amount', 'total_tax_amount', 'total_amount', 'freight_amount'}
    row_to_insert = clean_numeric_fields(row_to_insert, top_level_numeric_keys)
    
    line_item_numeric_keys = {'quantity', 'unit_price', 'amount'}
    row_to_insert['line_items'] = [clean_numeric_fields(item, line_item_numeric_keys) for item in row_to_insert.get('line_items', [])]
    
    vat_numeric_keys = {'tax_rate', 'tax_amount', 'amount', 'total_amount'}
    row_to_insert['vat'] = [clean_numeric_fields(item, vat_numeric_keys) for item in row_to_insert.get('vat', [])]
    
    return row_to_insert

def load_to_bigquery(row_to_insert: dict) -> None:
    """Its only job is to load a dictionary row into the destination BigQuery table."""
    table_id = f"{SETTINGS.PROJECT_ID}.{SETTINGS.BQ_DATASET_ID}.{SETTINGS.BQ_TABLE_ID}"
    errors = bq_client.insert_rows_json(table_id, [row_to_insert])
    if errors:
        print(f"BigQuery insertion errors: {errors}")
        raise RuntimeError(f"BigQuery insertion failed: {errors}")
    print("Successfully inserted row into BigQuery.")

def archive_file(bucket_name: str, full_gcs_path: str) -> None:
    """Its only job is to move a file from the raw bucket to the processed bucket."""
    source_bucket = storage_client.bucket(bucket_name)
    destination_bucket = storage_client.bucket(f"{bucket_name.replace('-raw', '')}-processed")
    source_blob = source_bucket.blob(full_gcs_path)

    # We are now copying the raw file that was just processed successfully,
    # then deleting it from the raw bucket and adding it to the processed bucket.
    source_bucket.copy_blob(source_blob, destination_bucket, full_gcs_path)
    source_blob.delete()
    print(f"Archived file to processed bucket.")

# --- MAIN FUNCTION (The Orchestrator) ---

@functions_framework.cloud_event
def process_invoice(cloud_event):
    """
    The main entry point. It orchestrates the entire invoice processing pipeline
    by calling the helper functions in the correct order.
    """
    try:
        # Step 1: Extract file info from the trigger event.
        file_info = parse_pubsub_message(cloud_event.data)
        
        # Step 2: Process the document with the AI.
        document = process_document_with_ai(file_info["gcs_uri"])
        
        # Step 3: Transform the AI response into a clean data record.
        final_row = transform_ai_response(document)
        
        # Step 4: Add our application's metadata to the final record.
        final_row.update({
            'event_id': file_info["event_id"],
            'source_gcs_path': file_info["gcs_uri"],
            'user_id': file_info["user_id"],
            'processed_timestamp': file_info["timestamp"],
            'status': 'Pending_Approval'
        })
        
        print(f"Final prepared data for BigQuery: {final_row}")
        
        # Step 5: Load the clean record into our data warehouse.
        load_to_bigquery(final_row)
        
        # Step 6: Archive the original file now that the process is complete.
        archive_file(file_info["bucket_name"], file_info["full_gcs_path"])
        
    except Exception as e:
        print(f"FATAL: A critical error occurred in the pipeline: {e}")
        # Re-raising the exception is important for Cloud Functions error reporting.
        raise