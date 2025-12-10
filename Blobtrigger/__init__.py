import logging
import azure.functions as func
import pandas as pd
import io
from datetime import datetime
from azure.storage.blob import BlobServiceClient


connection_string = ""
blob_service_client = BlobServiceClient.from_connection_string(connection_string)

def main(blob: func.InputStream):
    file_name = blob.name.split("/")[-1]
    logging.info(f"Processing file: {file_name}")

    try:
        
        blob_bytes = blob.read()
        if not blob_bytes:
            logging.warning(f"Blob {file_name} is empty. Skipping...")
            return

        blob_text = io.StringIO(blob_bytes.decode("utf-8"))
        df = pd.read_csv(blob_text)

        required_columns = ['txn_id', 'customer_id', 'amount', 'date']

       
        if not all(col in df.columns for col in required_columns):
            logging.info(f"File {file_name} is invalid. Moving to invalid-files.")
            invalid_client = blob_service_client.get_blob_client(container="invalid-files", blob=file_name)
            invalid_client.upload_blob(blob_bytes, overwrite=True)
            return

        
        logging.info(f"File {file_name} is valid. Moving to processed-files.")
        processed_client = blob_service_client.get_blob_client(container="processed-files", blob=file_name)
        processed_client.upload_blob(blob_bytes, overwrite=True)

        log_data = {
            "file": file_name,
            "rows": len(df),
            "processed_at": str(datetime.utcnow())
        }
        log_client = blob_service_client.get_blob_client(container="logs", blob=file_name.replace(".csv", ".json"))
        log_client.upload_blob(str(log_data), overwrite=True)

        logging.info(f"Processing completed for {file_name}")

    except Exception as e:
        logging.error(f"Error processing {file_name}: {e}")
