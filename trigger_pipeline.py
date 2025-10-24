import requests
import base64
import time
import json
import sys
import configs




def trigger_pipeline():
    client_id = configs.DATAOPS_CLIENT_ID
    client_secret = configs.DATAOPS_CLIENT_SECRET
    username = configs.DATAOPS_USERNAME
    password = configs.DATAOPS_PASSWORD
    pipeline_id = "dbff78cf-405f-4f56-b716-e7ecaa537783"

    auth_url = "http://192.168.6.205:6055/dataopssecurity/oauth2/token"
    basic_auth_str = f"{client_id}:{client_secret}"
    base64_auth_str = base64.b64encode(basic_auth_str.encode()).decode()

    headers = {
        "Authorization": f"Basic {base64_auth_str}",
        "Content-Type": "application/x-www-form-urlencoded"
    }

    payload = {
        "username": username,
        "password": password,
        "grant_type": "password"
    }

    response = requests.post(auth_url, headers=headers, data=payload)
    if response.status_code != 200:
        print(f"Auth failed: {response.status_code}, {response.text}")
        sys.exit(1)

    access_token = response.json().get("access_token")
    print(" Authentication successful")

    pipeline_url = "http://192.168.6.205:6055/piper/jobs"
    pipe_headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json",
    }
    pipe_payload = {
        "pipelineId": pipeline_id,
        "runName": "Trigger Pipeline using Github actions",
    }

    print(" Triggering Pipeline...")
    pipeline_response = requests.post(pipeline_url, headers=pipe_headers, json=pipe_payload)

    if pipeline_response.status_code != 200:
        print(f"Pipeline trigger failed: {pipeline_response.status_code}, {pipeline_response.text}")
        sys.exit(1)

    pipeline_data = pipeline_response.json()
    pipeline_run_id = pipeline_data.get("id") or pipeline_data.get("jobId")
    print(f" Pipeline triggered successfully, Run ID: {pipeline_run_id}")

    return access_token, pipeline_run_id


def pipeline_status(bearer_token, pipeline_run_id):
    status_url = f"http://192.168.6.205:6055/piper/jobs/{pipeline_run_id}/status"
    headers = {
        "Authorization": f"Bearer {bearer_token}",
        "Content-Type": "application/json",
    }

    print(" Checking pipeline status every 60 seconds...")

    while True:
        response = requests.get(status_url, headers=headers)
        if response.status_code != 200:
            print(f" Status check failed: {response.status_code}, {response.text}")
            sys.exit(1)

        try:
            data = response.json()
        except json.JSONDecodeError:
            print("Failed to parse JSON response")
            sys.exit(1)

        status = data.get("status")
        print(f"Pipeline Status: {status}")

        if status in ["COMPLETED", "FAILED", "ERROR"]:
            print(f" Pipeline finished with status: {status}")
            return status

        time.sleep(60)  


token, run_id = trigger_pipeline()
final_status = pipeline_status(token, run_id)

if final_status == "COMPLETED":
    sys.exit(0)
else:
    sys.exit(1)
