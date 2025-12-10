import os
import json
import logging
from datetime import datetime
import azure.functions as func
from azure.cosmos import CosmosClient, exceptions

# ----------------------------
# Cosmos DB Config
# ----------------------------
COSMOS_ENDPOINT = os.environ.get("COSMOS_CONN_STRING")
COSMOS_KEY = os.environ.get("COSMOS_KEY")
COSMOS_DB = os.environ.get("COSMOS_DB_NAME", "BankDB")
COSMOS_CONTAINER = os.environ.get("COSMOS_ALERTS_CONTAINER", "FraudAlerts")

# Singleton Cosmos client
_cosmos_client = None
def get_cosmos_client():
    global _cosmos_client
    if _cosmos_client is None:
        _cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
    return _cosmos_client

# ----------------------------
# Insert alert into Cosmos DB
# ----------------------------
def insert_alert(alert: dict):
    try:
        client = get_cosmos_client()
        db = client.get_database_client(COSMOS_DB)
        container = db.get_container_client(COSMOS_CONTAINER)
        container.upsert_item(alert)
        logging.info(f"Alert inserted in Cosmos DB: {alert['id']}")
    except exceptions.CosmosHttpResponseError as e:
        logging.error(f"Cosmos insert failed: {e}")

# ----------------------------
# Fraud detection logic
# ----------------------------
def analyze_transaction(txn_data: dict) -> dict:
    alert = {}
    if txn_data.get("TransactionAmount", 0) >= 50000:  # High-value threshold
        alert = {
            "id": f"{txn_data['TransactionID']}_HighValue",
            "alertType": "High-value transaction",
            "txn_id": txn_data["TransactionID"],
            "amount": txn_data["TransactionAmount"],
            "txnType": txn_data.get("TransactionType", ""),
            "customerId": txn_data.get("CustomerID", ""),
            "location": txn_data.get("Location", ""),
            "deviceId": txn_data.get("ATMID", ""),
            "timestamp": txn_data.get("TransactionTime", datetime.utcnow().isoformat()),
            "sourceFile": txn_data.get("sourceFile", "atm/upi_transaction.csv")
        }
    return alert

# ----------------------------
# Main function (EventGrid trigger)
# ----------------------------
def main(events: list, outputQueueItem: func.Out[str]):
    logging.info("DetectFraudFunction triggered")
    for event in events:
        try:
            # Ensure 'data' exists
            txn_data = event.get("data")
            if not txn_data:
                logging.error("No data found in event")
                continue

            # Analyze transaction
            alert = analyze_transaction(txn_data)

            if alert:
                # Insert into Cosmos DB
                insert_alert(alert)
                # Send to Service Bus output
                outputQueueItem.set(json.dumps(alert))
                logging.info(f"Fraud alert sent: {alert['id']}")
            else:
                logging.info(f"Transaction normal: {txn_data.get('TransactionID', 'unknown')}")

        except Exception as e:
            logging.error(f"Error processing event: {e}")
