import os
import json
import requests
import logging
from datetime import datetime, timedelta

# bucket_name = os.environ['GCS_BUCKET']
# bucket_name = "australia-southeast1-ltww-b577f002-bucket"

current_directory = os.path.dirname(os.path.abspath(__file__))
# credentials_path = 

# TOKEN_FILE_PATH = f"/{bucket_name}/dags/credentials/token_data.json"
# CREDENTIALS_PATH = f"/{bucket_name}/dags/credentials/tab_creds.json"

TOKEN_FILE_PATH = os.path.join(current_directory, '..', 'credentials', 'token_data.json')
CREDENTIALS_PATH = os.path.join(current_directory, '..', 'credentials', 'tab_creds.json')

def save_token_to_file(token_data, file_path):
    """Save token data to a local file."""
    with open(file_path, 'w') as f:
        json.dump(token_data, f)

def load_token_from_file(file_path):
    """Load token data from a local file."""
    if not os.path.exists(file_path):
        return None

    with open(file_path, 'r') as f:
        token_data = json.load(f)
    return token_data

def get_access_token():
    """Get the access token from the TAB Studio."""
    
    # Load token from local file
    # token_data = load_token_from_file(TOKEN_FILE_PATH)

    # # Check if token is still valid
    # if token_data:
    #     expiration_time = datetime.fromisoformat(token_data['expiration_time'])
    #     if datetime.now() < expiration_time:
    #         return token_data['access_token']

    # If token is invalid or doesn't exist, request a new one
    url = 'https://api.beta.tab.com.au/oauth/token'

    with open(CREDENTIALS_PATH, 'r') as f:
        payload = json.load(f)

    headers = {'Content-Type': 'application/x-www-form-urlencoded'}

    
    response = requests.post(url, data=payload, headers=headers)
    response.raise_for_status()
    json_response = response.json()
    

    # Calculate the expiration time
    expires_in_seconds = json_response['expires_in']
    expiration_time = datetime.now() + timedelta(seconds=expires_in_seconds)

    # Save token data to local file
    token_data = {
        'access_token': json_response['access_token'],
        'token_type': json_response['token_type'],
        'refresh_token': json_response['refresh_token'],
        'expiration_time': expiration_time.isoformat()
    }
    # save_token_to_file(token_data, TOKEN_FILE_PATH)

    return json_response['access_token']

