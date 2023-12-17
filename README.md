### Big data pipeline in GCP using PySpark

**GCP services used**: Cloud Storage, Dataproc & BigQuery

**Prerequisites**:
1. Create a service account with following roles:
    - BigQuery Admin
    - Dataproc Administrator
    - Editor
    - Storage Admin
2. Upload Python file in GCS for Dataproc to execute (`transform.py`)
3. Create dataset in BigQuery

**Data pipeline**:
1. Fetch data in JSON from a [Covid-19 tracking API](https://covidtracking.com/data/api/version-2)
2. Write data to a bucket in Google Cloud Storage
3. Spin up PySpark job to transform data using Dataproc
4. Transformed data written to BigQuery
5. Spark cluster auto-terminates (deleted) after transformation
