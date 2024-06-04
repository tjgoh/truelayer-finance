from auth import get_access_token
import requests
import psycopg2
from datetime import datetime, timedelta
import os
from dotenv import load_dotenv

load_dotenv(override=True)

def list_all_accounts():
    url = f"{os.getenv("DATA_API")}/data/v1/accounts"
    headers = {
    'Authorization': f'Bearer {get_access_token()}'
    }

    response = requests.request("GET", url, headers=headers)

    accounts = response.json()["results"]

    for account in accounts:
        url = f"{os.getenv("DATA_API")}/data/v1/accounts/{account["account_id"]}/balance"

        headers = {
        'Authorization': f'Bearer {get_access_token()}'
        }

        response = requests.request("GET", url, headers=headers)
        data = response.json()["results"][0]

        account["balance_update_timestamp"] = data["update_timestamp"]
        account["available"] = data["available"]
        account["current"] = data["current"]
        account["overdraft"] = data["overdraft"]

    return accounts

def load_accounts(accounts):
    conn = psycopg2.connect(os.getenv('POSTGRES_URI'))
    cur = conn.cursor()

    # list of tuples for each account 
    insert_data = [
        (
            account["account_id"],
            account["balance_update_timestamp"],
            account["account_type"],
            account["display_name"],
            account["currency"],
            account["account_number"]["iban"],
            account["account_number"]["swift_bic"],
            account["account_number"]["number"],
            account["account_number"]["sort_code"],
            account["provider"]["display_name"],
            account["provider"]["provider_id"],
            account["provider"]["logo_uri"],
            account["available"],
            account["current"],
            account["overdraft"]
        )
        for account in accounts
    ]

    query = """
    INSERT INTO accounts (id, balance_update_timestamp, account_type, display_name, currency, 
                                account_number_iban, account_number_swift_bic, account_number_number, 
                                account_number_sort_code, provider_display_name, provider_provider_id, provider_logo_uri, available, current, overdraft)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE
    SET balance_update_timestamp = EXCLUDED.balance_update_timestamp,
        account_type = EXCLUDED.account_type,
        display_name = EXCLUDED.display_name,
        currency = EXCLUDED.currency,
        account_number_iban = EXCLUDED.account_number_iban,
        account_number_swift_bic = EXCLUDED.account_number_swift_bic,
        account_number_number = EXCLUDED.account_number_number,
        account_number_sort_code = EXCLUDED.account_number_sort_code,
        provider_display_name = EXCLUDED.provider_display_name,
        provider_provider_id = EXCLUDED.provider_provider_id,
        provider_logo_uri = EXCLUDED.provider_logo_uri,
        available = EXCLUDED.available,
        current = EXCLUDED.current,
        overdraft = EXCLUDED.overdraft
    """
    cur.executemany(query, insert_data)
    conn.commit()

    cur.close()
    conn.close()

def transations(accounts):
    insert_data = []

    for account in accounts:
        today = datetime.now()
        formatted_today = today.strftime('%Y-%m-%d')
        seven_days_ago = today - timedelta(days=7)
        formatted_seven_days_ago = seven_days_ago.strftime('%Y-%m-%d')

        url = f"{os.getenv("DATA_API")}/data/v1/accounts/{account["account_id"]}/transactions?from={formatted_seven_days_ago}&to={formatted_today}"

        headers = {
        'Authorization': f'Bearer {get_access_token()}'
        }

        response = requests.request("GET", url, headers=headers)
        transaction_data = response.json()["results"]

        insert_data += [
            (
                transactions["transaction_id"],
                account["account_id"],
                transactions["timestamp"],
                transactions["description"],
                transactions["transaction_type"],
                transactions["transaction_category"],
                transactions["transaction_classification"],
                transactions["amount"],
                transactions["currency"]
            )
            for transactions in transaction_data
        ]
    
    conn = psycopg2.connect(os.getenv('POSTGRES_URI'))
    cur = conn.cursor()

    query = """
    INSERT INTO transactions (id, account_id, timestamp, description, transaction_type, transaction_category, 
                            transaction_classification, amount, currency)
    VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
    ON CONFLICT (id) DO UPDATE
    SET account_id = EXCLUDED.account_id,
        timestamp = EXCLUDED.timestamp,
        description = EXCLUDED.description,
        transaction_type = EXCLUDED.transaction_type,
        transaction_category = EXCLUDED.transaction_category,
        transaction_classification = EXCLUDED.transaction_classification,
        amount = EXCLUDED.amount,
        currency = EXCLUDED.currency
    """

    cur.executemany(query, insert_data)
    conn.commit()

    cur.close()
    conn.close()

def main():
    accounts = list_all_accounts()
    load_accounts(accounts)
    transations(accounts)

if __name__ == "__main__":
    main()
