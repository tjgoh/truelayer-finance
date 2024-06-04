import psycopg2
import os
from dotenv import load_dotenv

load_dotenv(override=True)

command = (
    """
    CREATE TABLE IF NOT EXISTS accounts (
        id VARCHAR PRIMARY KEY,
        balance_update_timestamp TIMESTAMP,
        account_type VARCHAR,
        display_name VARCHAR,
        currency VARCHAR, 
        account_number_iban VARCHAR,
        account_number_swift_bic VARCHAR,
        account_number_number VARCHAR,
        account_number_sort_code VARCHAR(8),
        provider_display_name VARCHAR,
        provider_provider_id VARCHAR,
        provider_logo_uri VARCHAR,
        available NUMERIC,
        current NUMERIC,
        overdraft NUMERIC
    );

    CREATE TABLE IF NOT EXISTS transactions (
        id VARCHAR PRIMARY KEY,
        account_id VARCHAR,
        timestamp TIMESTAMP,
        description VARCHAR,
        transaction_type VARCHAR,
        transaction_category VARCHAR,
        transaction_classification VARCHAR,
        amount NUMERIC,
        currency varchar
    )
    """
)

conn = psycopg2.connect(os.getenv('POSTGRES_URI'))
cur = conn.cursor()
cur.execute(command)
conn.commit()
conn.close()
cur.close()