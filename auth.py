import requests
import json
import time
import os
from dotenv import load_dotenv

load_dotenv(override=True)

url = os.getenv('AUTH_API')
client_id = os.getenv('CLIENT_ID')
client_secret = os.getenv('CLIENT_SECRET')

def get_credentials_from_code():
    grant_type = "authorization_code"
    redirect_uri = "https%3A%2F%2Fconsole.truelayer.com%2Fredirect-page"
    code = os.getenv('CODE')

    payload = f'grant_type={grant_type}&client_id={client_id}&client_secret={client_secret}&redirect_uri={redirect_uri}&code={code}'

    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    credentials = response.json()

    return credentials

def get_credentials_from_refresh_token(credentials):
    grant_type = "refresh_token"
    refresh_token = credentials["refresh_token"]
    
    payload = f'grant_type={grant_type}&client_id={client_id}&client_secret={client_secret}&refresh_token={refresh_token}'
    headers = {
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.request("POST", url, headers=headers, data=payload)
    credentials = response.json()

    return credentials

def get_access_token():
    credentials = None

    if not os.path.isfile("credentials.json"):
        credentials = get_credentials_from_code()
    else:
        with open("credentials.json", encoding='utf-8-sig') as f:
            prev_credentials = json.load(f)

            if int(time.time()) > prev_credentials["expires_at"]:
                credentials = get_credentials_from_refresh_token(prev_credentials)
            else:
                credentials = prev_credentials
                return credentials["access_token"]
    if credentials:  
        credentials["expires_at"] = credentials["expires_in"] + int(time.time())

        with open("credentials.json", "w") as output:
            json.dump(credentials, output, sort_keys=True)
        
        return credentials["access_token"]

if __name__ == "__main__":
    get_access_token()