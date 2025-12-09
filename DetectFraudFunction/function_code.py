import os, json, logging
from datetime import datetime
import azure.functions as func
from azure.cosmos import CosmosClient, PartitionKey
from azure.servicebus import ServiceBusClient, ServiceBusMessage
from dateutil import parser as dateparser

# Config (use local.settings.json or Azure App Settings)
COSMOS_ENDPOINT = os.environ.get("COSMOS_CONN_STRING")
COSMOS_KEY = os.environ.get("COSMOS_KEY")
COSMOS_DB = os.environ.get("COSMOS_DB_NAME", "BankDB")
COSMOS_CONTAINER = os.environ.get("COSMOS_ALERTS_CONTAINER", "FraudAlerts")
SERVICEBUS_CONN = os.environ.get("SERVICE_BUS_CONN")
SERVICEBUS_QUEUE = os.environ.get("SERVICEBUS_QUEUE", "fraud-notifications")
HIGH_VALUE_THRESHOLD = 50000

_cosmos_client = None
_servicebus_client = None

def get_cosmos_client():
    global _cosmos_client
    if _cosmos_client is None:
        _cosmos_client = CosmosClient(COSMOS_ENDPOINT, COSMOS_KEY)
        db = _cosmos_client.create_database_if_not_exists(id=COSMOS_DB)
        db.create_container_if_not_exists(id=COSMOS_CONTAINER, partition_key=PartitionKey(path="/customerId"))
    return _cosmos_client

def get_servicebus_client():
    global _servicebus_client
    if _servicebus_client is None:
        _servicebus_client = ServiceBusClient.from_connection_string(SERVICEBUS_CONN)
    return _servicebus_client

def is_high_value(amount):
    try:
        return float(amount) >= HIGH_VALUE_THRESHOLD
    except:
        return False

def detect_anomaly(txn):
    reasons = []
    if is_high_value(txn.get("TransactionAmount", 0)):
        reasons.append("HIGH_VALUE")
    return len(reasons) > 0, reasons

def insert_alert(alert):
    client = get_cosmos_client()
    db = client.get_database_client(COSMOS_DB)
    container = db.get_container_client(COSMOS_CONTAINER)
    container.create_item(body=alert)

def send_notification(payload):
    sb = get_servicebus_client()
    with sb.get_queue_sender(queue_name=SERVICEBUS_QUEUE) as sender:
        sender.send_messages(ServiceBusMessage(json.dumps(payload)))

def main(event: func.EventGridEvent):
    logging.info("Event received")
    txn = event.get_json()
    anomalous, reasons = detect_anomaly(txn)
    if anomalous:
        alert = {
            "id": txn.get("TransactionID", f"alert-{datetime.utcnow().isoformat()}"),
            "customerId": txn.get("CustomerID"),
            "transactionId": txn.get("TransactionID"),
            "amount": txn.get("TransactionAmount"),
            "reasons": reasons,
            "status": "OPEN",
            "createdAt": datetime.utcnow().isoformat()
        }
        insert_alert(alert)
        send_notification({"alertId": alert["id"], "customerId": alert["customerId"], "reasons": reasons})
        logging.info(f"Fraud alert created: {alert['id']}")
    else:
        logging.info("Transaction normal")

# Local test block
if __name__ == "__main__":
    import json

    sample_txn = {
        "TransactionID": "TXN1001",
        "CustomerID": "CUST001",
        "TransactionAmount": 75000,
        "DeviceID": "DEV123",
        "Location": "Kolkata"
    }

    class DummyEvent:
        def get_json(self):
            return sample_txn

    main(DummyEvent())
