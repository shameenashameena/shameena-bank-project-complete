import datetime
import logging
import azure.functions as func
import pyodbc


def main(mytimer: func.TimerRequest) -> None:
    utc_now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    logging.info(f"customer_sync trigger started at {utc_now}")

    # Azure SQL connection
    conn = pyodbc.connect(
        "Driver={ODBC Driver 18 for SQL Server};"
        "Server=tcp:shameenaserver.database.windows.net,1433;"
        "Database=shameenasql;"
        "Uid=shameena;"
        "Pwd=snd@786123;"
        "Encrypt=yes;"
        "TrustServerCertificate=no;"
        "Connection Timeout=30;"
    )
    cursor = conn.cursor()

    # Read all DimCustomer records
    cursor.execute("""
        SELECT CustomerID, CustomerName, DOB, Gender, Address, Email, Status, EffectiveDate, EndDate, IsCurrent
        FROM DimCustomer
    """)
    customers = cursor.fetchall()

    for cust in customers:
        customer_id = cust.CustomerID
        customer_name = cust.CustomerName or "Unknown"
        status = cust.Status or "Active"
        effective_date = utc_now.date()
        is_current = cust.IsCurrent

        # Only update if current
        if is_current:
            # Expire old current record
            cursor.execute("""
                UPDATE DimCustomer
                SET EndDate = ?, IsCurrent = 0
                WHERE CustomerID = ? AND IsCurrent = 1
            """, effective_date, customer_id)

            # Insert new active record
            cursor.execute("""
                INSERT INTO DimCustomer 
                (CustomerID, CustomerName, DOB, Gender, Address, Email, Status, EffectiveDate, EndDate, IsCurrent)
                VALUES (?, ?, ?, ?, ?, ?, ?, ?, NULL, 1)
            """, customer_id, customer_name, cust.DOB, cust.Gender, cust.Address, cust.Email, status, effective_date)

    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Customer sync completed successfully using DimCustomer only!")
