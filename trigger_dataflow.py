import config
import requests
import base64

client_id = config.DATAOPS_CLIENT_ID
client_secret = config.DATAOPS_CLIENT_SECRET
username = config.DATAOPS_USERNAME
password = config.DATAOPS_PASSWORD



# Encode client_id:client_secret for Basic Auth header
basic_auth_str = f"{client_id}:{client_secret}"
basic_auth_bytes = basic_auth_str.encode('utf-8')
base64_bytes = base64.b64encode(basic_auth_bytes)
base64_auth_str = base64_bytes.decode('utf-8')

# Endpoint URL
auth_url = "https://poc.datagaps.com/dataopssecurity/oauth2/token"



# Headers with Basic Auth and content type
headers = {
    "Authorization": f"Basic {base64_auth_str}",
    "Content-Type": "application/x-www-form-urlencoded"
}

# Form data payload
payload = {
    "username": username,
    "password": password,
    "grant_type": "password"
}

response = requests.post(auth_url, headers=headers, data=payload)
dataflow_id='23f975aa-1982-4be1-9b33-e203a4aea2ea'
if response.status_code == 200:
    access_token = response.json().get("access_token")
    print("Access token:", access_token)

    # Trigger the dataflow
    trigger_url = f"https://poc.datagaps.com/DataFlowService/api/v1.0/dataFlows/executeDataFlow?dataflowId={dataflow_id}"
    trigger_headers = {
        "Authorization": f"Bearer {access_token}"
    }

    trigger_response = requests.post(trigger_url, headers=trigger_headers)

    if trigger_response.status_code == 200:
        run_id = trigger_response.json().get("dataFlowRunId")
        print(f"Dataflow triggered, run ID: {run_id}")
    else:
        print(f"Failed to trigger dataflow: {trigger_response.status_code} - {trigger_response.text}")

else:
    print(f"Failed to get token: {response.status_code} - {response.text}")
