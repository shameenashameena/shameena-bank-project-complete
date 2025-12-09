import datetime
import logging

import azure.functions as func
import pyodbc


def main(mytimer: func.TimerRequest) -> None:
    utc_now = datetime.datetime.utcnow().replace(tzinfo=datetime.timezone.utc)
    logging.info(f"account_status trigger started at {utc_now}")

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

    # Read all DimAccount records
    cursor.execute("""
        SELECT AccountNumber, CustomerID, BranchID, ProductID, AccountType, Status, OpenDate, CloseDate
        FROM DimAccount
    """)
    accounts = cursor.fetchall()

    for acc in accounts:
        account_number = acc.AccountNumber
        status = acc.Status
        open_date = acc.OpenDate
        close_date = acc.CloseDate

        # Example rule: Close accounts older than 30 days
        if status == "Active" and open_date and (utc_now.date() - open_date).days > 30:
            cursor.execute("""
                UPDATE DimAccount
                SET Status = 'Closed', CloseDate = ?
                WHERE AccountNumber = ?
            """, utc_now.date(), account_number)

    conn.commit()
    cursor.close()
    conn.close()

    logging.info("Account status sync completed successfully using DimAccount only!")

    logging.info("Account status sync completed successfully using DimAccount only!")
