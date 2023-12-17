"""
Create a bucket (if not exists) in Google Cloud Storage
Download JSON file in bucket (created in preceding step)

Execute spark job programmatically
Spin up dataproc cluster and submit job for execution
Delete the dataproc cluster upon successful completion of job
"""

import requests
from google.cloud import storage
import json
import io
from google.cloud import dataproc_v1 as dataproc
from time import sleep

API_URL = "https://api.covidtracking.com/v2/us/daily.json"
GCP_BUCKET_NAME = "covid19data_demo"
BQ_WRITE_BUCKET_NAME = "covid19data_demo_bq_write"
DATA_DESTINATION_NAME = "covidtrackingdata.json"
PROJECT_ID = "concise-display-407818"
CLUSTER_NAME = "cluster-07bc"
REGION = "us-central1"
DATAPROC_JOB_FILE = "gs://covid19data_demo/transform.py"
SERVICE_ACCOUNT_JSON_PATH = "/home/infernape/gcp-projects/covid19datapipeline/service_account_secrets.json"
client = storage.Client.from_service_account_json(SERVICE_ACCOUNT_JSON_PATH)

# Create the bucket if it doesn't exist
def create_bucket_if_not_exists(bucket_name, project_id):
    try:
        client.get_bucket(bucket_name)
        print(f"Bucket {bucket_name} already exists.")
    except Exception as e:
        bucket = client.create_bucket(bucket_name, project=project_id)
        print(f"Bucket {bucket_name} created.")

create_bucket_if_not_exists(BQ_WRITE_BUCKET_NAME, PROJECT_ID)

# Fetch JSON data from the API
response = requests.get(API_URL)
response.raise_for_status()
data = response.json()["data"]
json_data = json.dumps(data)

# Create a bytes stream and write the JSON string to it
bytes_stream = io.BytesIO()
bytes_stream.write(json_data.encode('utf-8'))
bytes_stream.seek(0)

# Initialize the GCS client with service account credentials
bucket = client.get_bucket(GCP_BUCKET_NAME)
blob = bucket.blob(DATA_DESTINATION_NAME)

# delete JSON file if already exists
if blob.exists():
    blob.delete()
    print(f"Blob {DATA_DESTINATION_NAME} deleted from {GCP_BUCKET_NAME}.")

# Save the JSON data to GCS
blob.upload_from_file(bytes_stream, content_type='application/json')
print(f"JSON data saved to gs://{GCP_BUCKET_NAME}/{DATA_DESTINATION_NAME}")

# Initialize Dataproc and Storage clients
cluster_client = dataproc.ClusterControllerClient.from_service_account_file(SERVICE_ACCOUNT_JSON_PATH,client_options={'api_endpoint': f'{REGION}-dataproc.googleapis.com:443'})
job_client = dataproc.JobControllerClient.from_service_account_file(SERVICE_ACCOUNT_JSON_PATH,client_options={'api_endpoint': f'{REGION}-dataproc.googleapis.com:443'})

# Create cluster config
cluster_config = {
  "project_id": PROJECT_ID,
  "cluster_name": CLUSTER_NAME,
  "config": {
    "config_bucket": "",
    "gce_cluster_config": {
      "service_account_scopes": [
        "https://www.googleapis.com/auth/cloud-platform"
      ],
      "network_uri": "default",
      "subnetwork_uri": "",
      "internal_ip_only": False,
      "zone_uri": "",
      "metadata": {},
      "tags": [],
      "shielded_instance_config": {
        "enable_secure_boot": False,
        "enable_vtpm": False,
        "enable_integrity_monitoring": False
      }
    },
    "master_config": {
      "num_instances": 1,
      "machine_type_uri": "n2-standard-4",
      "disk_config": {
        "boot_disk_type": "pd-standard",
        "boot_disk_size_gb": 500,
        "num_local_ssds": 0
      },
      "min_cpu_platform": "",
      "image_uri": ""
    },
    "software_config": {
      "image_version": "2.1-debian11",
      "properties": {
        "dataproc:dataproc.allow.zero.workers": "true"
      },
      "optional_components": []
    },
    "lifecycle_config": {},
    "initialization_actions": [],
    "encryption_config": {
      "gce_pd_kms_key_name": ""
    },
    "autoscaling_config": {
      "policy_uri": ""
    },
    "endpoint_config": {
      "enable_http_port_access": False
    },
    "security_config": {
      "kerberos_config": {}
    }
  },
  "labels": {},
  "status": {},
  "status_history": [
    {}
  ],
  "metrics": {}
}

# Create cluster
request = dataproc.CreateClusterRequest(
        project_id=PROJECT_ID,
        region=REGION,
        cluster=cluster_config,
)
try:
    print("Creating cluster...")
    operation = cluster_client.create_cluster(request = request)
    result = operation.result()
except Exception as e:
    print(e) 

# Ensure cluster is up before submitting a job
cluster_request = dataproc.GetClusterRequest(
        project_id=PROJECT_ID,
        region=REGION,
        cluster_name=CLUSTER_NAME,
)   
while True:
    cluster_info = cluster_client.get_cluster(request = cluster_request)
    if cluster_info.status.state.name == 'RUNNING':
        print("Cluster running...")
        break
    sleep(10)
    
# Submit job to Dataproc
print("Submitting job...")
job_config = {
    'placement': {
        'cluster_name': CLUSTER_NAME
    },
    'pyspark_job': {
        'main_python_file_uri': DATAPROC_JOB_FILE
    }
}
submit_job_request = dataproc.SubmitJobRequest(
        project_id=PROJECT_ID,
        region=REGION,
        job = job_config
)
job = job_client.submit_job_as_operation(request = submit_job_request)
job_id = job.result().reference.job_id

# Wait for job completion
job_request = dataproc.GetJobRequest(
        project_id=PROJECT_ID,
        region=REGION,
        job_id=job_id,
)
while True:
    job_info = job_client.get_job(request = job_request)
    if job_info.status.state.name == 'DONE':
        print("Job completed...")
        break
    sleep(10)

# Delete the cluster
print("Deleting cluster...")
operation = cluster_client.delete_cluster(
    request={
        "project_id": PROJECT_ID,
        "region": REGION,
        "cluster_name": CLUSTER_NAME,
    }
)
operation.result()
print("Cluster deleted...")