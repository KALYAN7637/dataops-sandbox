import requests
import base64

import time


bearer_token=''


def trigger_dataflow():
    global bearer_token
    client_id = "dataopssuite-restapi-client"
    client_secret = "BKG0@8lE"
    username = "vijrumbhan"
    password = "U2FsdGVkX190zofymDJqYKiiowPaEt9ganRAK8IgS4k="






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
    dataflow_id='209c545e-ac41-4126-ac4a-c2532e6f9172'
    if response.status_code == 200:
        access_token = response.json().get("access_token")
        bearer_token=access_token
        print("Access token:", access_token)

        # Trigger the dataflow
        trigger_url = f"https://poc.datagaps.com/DataFlowService/api/v1.0/dataFlows/executeDataFlow?dataflowId={dataflow_id}"
        trigger_headers = {
            "Authorization": f"Bearer {access_token}"
        }

        trigger_response = requests.post(trigger_url, headers=trigger_headers)

        if trigger_response.status_code == 200:
            print(trigger_response.json())
            run_id = trigger_response.json().get("dataFlowRunId")
            print(f"Dataflow triggered, run ID: {run_id}")

            return run_id
        else:
            print(f"Failed to trigger dataflow: {trigger_response.status_code} - {trigger_response.text}")

    else:
        print(f"Failed to get token: {response.status_code} - {response.text}")

df_run_id=trigger_dataflow()



def check_dataflow_status(bearer_token,df_run_id):
    status_url = f"https://poc.datagaps.com/DataFlowService/api/v1.0/dataFlows/dataflow-status?dataFlowRunId={df_run_id}"
    try:
        headers={'Authorization':'Bearer '+bearer_token}
        status_resp = requests.get(status_url, headers=headers)
        status_resp.raise_for_status()
        status_data = status_resp.json()
        return status_data.get("status", "").upper()
    except requests.RequestException as e:
        print(f"Error checking DataFlow status: {e}")
        return "ERROR"

print("Checking DataFlow status...")
while True:
    status = check_dataflow_status(bearer_token,df_run_id)
    print(f"DataFlow Status: {status}")
    if status in ["COMPLETED", "FAILED", "ERROR"]:
        break
    time.sleep(50)


print(status)
# After polling and getting the final status
print(f"Final DataFlow Status: {status}")

# Exit code based on status
if status == "COMPLETED":
    exit(0)  
elif status in ["FAILED", "ERROR"]:
    exit(1)  
else:
    exit(1)  



