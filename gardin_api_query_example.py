import requests
import json
import base64
from time import sleep, time
from typing import Dict, Tuple

"""
Example code demonstrating how to:
1. Authenticate with the Gardin API
2. Submit and monitor a query 
3. Download results to a CSV file

Before running this code, you must:
1. Install the requests library (see below)
2. Set your CLIENT_ID and CLIENT_SECRET constants below
3. Set the CSV_DOWNLOAD_PATH constant to your desired output location

To run the code:
1. Open a new terminal window
2. Navigate to where this file is saved:
   cd /path/to/your/code/directory
3. Install the requests library:
   pip install requests
4. Run the script:
   python3 gardin_api_query_simple_example.py
"""

# Required: Add your Gardin API credentials here
CLIENT_ID = '<YOUR_CLIENT_ID>'  # Replace with your client ID
CLIENT_SECRET = '<YOUR_CLIENT_SECRET>'  # Replace with your client secret

# Configuration constants
QUERY_STATUS_POLLING_WAIT_SECS = 10
CSV_DOWNLOAD_PATH_PREFIX = 'gardin_query_api_results_'  # Prefix for output files

def check_api_response(response: requests.Response, action: str) -> None:
    """
    Validate API response and raise exception if request failed.
    
    Args:
        response: Response object from requests
        action: Description of the API action for logging
    Raises:
        RuntimeError: If the API request was not successful
    """
    print(f"{action} - Response Status {response.status_code}")
    if not response.ok:
        print(f"Request Response Error Code {response.reason}")
        raise RuntimeError(f"API request failed: {response.reason}")

def get_auth_token() -> str:
    """
    Get authentication token using client credentials flow.
    
    Returns:
        str: Authentication token
    """
    login_url = "https://login.gardin.ag/oauth2/token"
    credentials = f"{CLIENT_ID}:{CLIENT_SECRET}"
    encoded_credentials = base64.b64encode(credentials.encode('utf-8')).decode('utf-8')
    
    auth_payload = {"grant_type": "client_credentials"}
    auth_headers = {
        'Authorization': f'Basic {encoded_credentials}',
        'Content-Type': 'application/x-www-form-urlencoded'
    }

    response = requests.post(login_url, headers=auth_headers, data=auth_payload)
    check_api_response(response, "Get Authentication token")
    return json.loads(response.text).get('access_token', '')

def submit_query(token: str, headers: Dict) -> str:
    """
    Submit a query to the Gardin API.
    
    Args:
        token: Authentication token
    Returns:
        str: Query ID for tracking the query status
    """
    query_url = "https://api.gardin.ag/v1/query"

    # Example query filter - 1 month of indices data
    query_payload = {
        "type": "indices",
        "filters": {
            "from": "2024-12-01T17:32:28Z",
            "to": "2024-12-30T00:23:46Z"
        }
    }

    response = requests.post(
        query_url, 
        headers=headers, 
        data=json.dumps(query_payload)
    )
    check_api_response(response, "Execute Query")
    return json.loads(response.text).get('queryId', '')

def monitor_query_status(query_id: str, headers: Dict) -> bool:
    """
    Monitor the status of a submitted query.
    
    Args:
        query_id: ID of the submitted query
        headers: Request headers containing authentication
    Returns:
        bool: True if query completed successfully, False otherwise
    """
    status_url = f"https://api.gardin.ag/v1/query/{query_id}/status/"
    
    while True:
        response = requests.get(status_url, headers=headers)
        check_api_response(response, "Get Processing status")
        status = json.loads(response.text).get('status', 'NO_STATUS_RETURNED')
        
        if status in ['SUBMITTED', 'IN_PROGRESS', 'RUNNING']:
            print(f"Waiting {QUERY_STATUS_POLLING_WAIT_SECS} seconds for query to complete - Current Status: {status}")
            sleep(QUERY_STATUS_POLLING_WAIT_SECS)
            continue
            
        if status == 'COMPLETED':
            print(f"Query completed with status: {status}")
            return True
            
        if status in ['FAILED', 'CANCELLED']:
            print(f"Query failed with status: {status}")
            return False

        print(f"Error:Unknown status -{status}")
        return False

def download_results(query_id: str, headers: Dict) -> str:
    """
    Get the signed URI for downloading query results.
    
    Args:
        query_id: ID of the completed query
        headers: Request headers containing authentication
    Returns:
        str: Signed URI for downloading results
    """
    download_url = f"https://api.gardin.ag/v1/query/{query_id}/result/download"
    response = requests.get(download_url, headers=headers)
    check_api_response(response, "Get URI for csv file")
    return json.loads(response.text).get('uri', '')

def save_results(signed_uri: str) -> None:
    """
    Download and save query results to a CSV file.
    
    Args:
        signed_uri: URI to download the results
    """
    csv_filename = f"{CSV_DOWNLOAD_PATH_PREFIX}{int(time())}.csv"
    print(f"Downloading csv file to {csv_filename}")
    response = requests.get(signed_uri)
    with open(csv_filename, 'wb') as f:
        f.write(response.content)
    print("Download Completed")

def main():
    """
    Main execution flow for querying the Gardin API and downloading results.
    """
    # 1. Get authentication token
    token = get_auth_token()

    # Setup headers for subsequent requests
    headers = {
        'Content-Type': 'application/json',
        'Accept': 'application/json',
        'Authorization': f'Bearer {token}'
    }
    
    # 2. Submit query
    query_id = submit_query(token, headers)

    print(f"Query submitted with ID: {query_id}")
    
    # 3. Monitor query status
    query_completed = monitor_query_status(query_id, headers)
    
    if query_completed:
        # 4. Get download URI
        signed_uri = download_results(query_id, headers)
        
        # 5. Download and save results
        save_results(signed_uri)
    else:
        print("Query did not complete successfully")

if __name__ == "__main__":
    main()
