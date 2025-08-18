import os
from fastapi import FastAPI, UploadFile, File, HTTPException
from google.cloud import storage
from dotenv import load_dotenv

# Load environment variables for local development
load_dotenv()

# --- CONFIGURATION & CLIENTS ---
PROJECT_ID = os.getenv("PROJECT_ID")
RAW_BUCKET = os.getenv("RAW_BUCKET")
storage_client = storage.Client(project=PROJECT_ID)

# --- FASTAPI APP ---
app = FastAPI(title="ExtractAI Uploader API")


@app.get("/")
def read_root():
    """ A simple root endpoint to confirm the API is running. """
    return {"status": "ok", "service": "Uploader API"}


@app.post("/upload/")
async def upload_invoice(file: UploadFile = File(...)):
    """
    Receives an invoice file and uploads it to the raw GCS bucket.
    The user's identity will be added later via an authentication dependency.
    """
    if not file.filename or not file.content_type == "application/pdf":
        raise HTTPException(status_code=400, detail="Invalid file. Must be a PDF with a filename.")

    try:
        # TODO: Replace with real user ID from Firebase Auth dependency
        user_id = "placeholder-user-id"
        gcs_path = f"{user_id}/{file.filename}"
        
        bucket = storage_client.get_bucket(RAW_BUCKET)
        blob = bucket.blob(gcs_path)

        print(f"Uploading file to gs://{RAW_BUCKET}/{gcs_path}")
        blob.upload_from_file(file.file)

        return {"status": "success", "filename": file.filename, "gcs_path": gcs_path}
    except Exception as e:
        print(f"Error during upload: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred during file upload.")