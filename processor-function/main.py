import base64
import json

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
    
    print(f"Processing file: {file_data['name']}")

    # Add this code inside your function, after the print statement

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