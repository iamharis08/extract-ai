import os
import base64
import json
from dotenv import load_dotenv
from google.cloud import documentai_v1 as documentai
from google.cloud import bigquery
from google.cloud import pubsub_v1

# This line loads the variables from your .env file into the environment
load_dotenv()

# --- CONFIGURATION ---
# We read from the environment instead of hardcoding
PROJECT_ID = os.getenv("PROJECT_ID") 
LOCATION = os.getenv("LOCATION")  # This must match the region of your processor
PROCESSOR_ID = os.getenv("PROCESSOR_ID") # Your Processor ID

# Initialize the clients for the services we'll use.
# We do this once, outside the function, for efficiency.
docai_client = documentai.DocumentProcessorServiceClient()
bq_client = bigquery.Client()
publisher = pubsub_v1.PublisherClient()
# --- END CONFIGURATION ---


def process_invoice(event, context):
    """
    This function is triggered by a message on a Pub/Sub topic.
    """
    print(f"This Function was triggered by messageId: {context.event_id}")

    # The actual message data is in the 'data' field of the event.
    # It's a base64-encoded string, so we need to decode it.
    pubsub_message_data = base64.b64decode(event['data']).decode('utf-8')
    
    # The decoded string is itself a JSON object with file details.
    file_data = json.loads(pubsub_message_data)
    
    # Get the full path of the file from the message data.
    full_path = file_data['name']
    bucket_name = file_data['bucket']

    # We expect the path to be in the format "user_id/filename".
    # We split the string by the '/' to get a list of its parts.
    parts = full_path.split('/')

    # Important: We add a check to make sure the file is in a user folder.
    # This prevents the function from crashing if a file is uploaded to the root.
    if len(parts) != 2:
        print(f"Error: The file '{full_path}' is not in a user folder. Skipping.")
        return  # Stop the function execution

    # This is a Python trick called "list unpacking".
    # It assigns the first item in the list to user_id and the second to filename.
    user_id, filename = parts

    print(f"Received file '{filename}' from user '{user_id}' in bucket '{bucket_name}'.")

    # --- Call Document AI Processor ---
    
    # Create the full GCS path to the invoice
    gcs_uri = f"gs://{bucket_name}/{full_path}"
    
    # Define the file type
    mime_type = "application/pdf"
    
    # Construct the full resource name of the processor
    processor_name = docai_client.processor_path(PROJECT_ID, LOCATION, PROCESSOR_ID)
    
    # Create the document object for the API request
    raw_document = documentai.RawDocument(
        gcs_uri=gcs_uri,
        mime_type=mime_type,
    )
    
    # Make the actual API call to Document AI
    print("Making request to Document AI processor...")
    request = documentai.ProcessRequest(
        name=processor_name,
        raw_document=raw_document
    )
    
    result = docai_client.process_document(request=request)
    document = result.document
    
    print(f"Document processing complete. Found {len(document.entities)} entities.")
    # --- End Call to Document AI ---

    # --- Parse the Document AI Response ---
    extracted_data = {}
    for entity in document.entities:
        key = entity.type_ # Keep original key, e.g., 'vendor_name'
        value = entity.mention_text.replace('\n', '')
        extracted_data[key] = value

    print("Successfully extracted data:")
    print(extracted_data)
    # --- End Parsing ---