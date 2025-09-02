# v1.1 - Testing Cloud Deploy Pipeline --------
import os
from functools import lru_cache
from fastapi import FastAPI, UploadFile, File, HTTPException, Depends
from google.cloud import storage
from pydantic_settings import BaseSettings

# --- SETTINGS MANAGEMENT ---
class Settings(BaseSettings):
    """ Defines application settings, read from .env file and environment. """
    PROJECT_ID: str
    RAW_BUCKET: str

    class Config:
        env_file = ".env"

@lru_cache()
def get_settings():
    """ Provides a cached instance of the settings. """
    return Settings(_env_file='.env', _env_file_encoding='utf-8')

# --- GOOGLE CLOUD CLIENTS (as dependencies) ---
def get_storage_client(settings: Settings = Depends(get_settings)):
    """ Provides a storage client, ensuring it's initialized with a project ID. """
    return storage.Client(project=settings.PROJECT_ID)

# --- FASTAPI APP ---
app = FastAPI(title="ExtractAI Uploader API")


@app.get("/")
def read_root():
    """ Health check endpoint. """
    return {"status": "TESTING CANARY", "service": "Uploader API"}


@app.post("/upload/")
async def upload_invoice(
    file: UploadFile = File(...),
    settings: Settings = Depends(get_settings),
    storage_client: storage.Client = Depends(get_storage_client)
):
    """
    Receives an invoice file, validates it, and uploads it to the raw GCS bucket.
    User identity will be added later via an authentication dependency.
    """
    if not file.filename or not file.content_type == "application/pdf":
        raise HTTPException(status_code=400, detail="Invalid file. Must be a PDF with a filename.")

    try:
        # TODO: Replace with real user ID from Firebase Auth dependency
        user_id = "test-user-id"
        gcs_path = f"{user_id}/{file.filename}"
        
        bucket = storage_client.get_bucket(settings.RAW_BUCKET)
        blob = bucket.blob(gcs_path)

        print(f"Uploading file to gs://{settings.RAW_BUCKET}/{gcs_path}")
        blob.upload_from_file(file.file)

        return {"status": "success", "filename": file.filename, "gcs_path": gcs_path}
    except Exception as e:
        print(f"Error during upload: {e}")
        raise HTTPException(status_code=500, detail="An unexpected error occurred during file upload.")