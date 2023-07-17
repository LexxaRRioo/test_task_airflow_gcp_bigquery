## About this repo
This repo contains my implementation of Airflow + GCP test task to apply to a Data Engineer position.

You can see task description, setup steps and some thought about future improvements below.

What I've implemented to accomplish the task:
* BigQuery tables creating and filling in
* BigQuery table dumping into GCS(S3) CSV file
* GCS file to SFTP transfering
* Config json file for environment-specific and other parameters
  
What I've made above the task:
* Idempotency of the tasks
* DAG callbacks (messages on success, retry, failure etc)
* SLA and timeout for the DAG
* Preparation steps to assure DAG would work
* Task group for preparation steps

---
## Task
Create an Airflow DAG with the following sequence:
1. Execute a BigQuery script that creates a table with data (a simple script of your choice)
2. Execute a BigQuery script that writes the results from the previous table to a table with historical data and partitions by execution date, where the historical results of the script execution from step 1 are stored.
3. Upload the results of the table from step 1 to GCS.
4. Create an empty file on GCS.
5. Copy an empty file from GCS to the SFTP server.
6. (The operation from step 5 triggers the execution of a certain API), you need to wait for the execution of this API
7. Transfer the data from step 3 to the SFTP server.

Clarifications:
- It is important to make sure that paths, GCP projects and other parameters can be specified in the configuration file depending on the environment.
- It is important to use the right operators, in the right way, real work wouldn't be checked
---
## SETUP
### 1. Configure project in cloud console 
1. Select or create a Cloud Platform project using the Cloud Console.
https://console.cloud.google.com/project
2. Enable billing for your project, as described in the Google Cloud documentation.
https://cloud.google.com/billing/docs/how-to/modify-project#enable_billing_for_a_project
3. Enable the API, as described in the Cloud Console documentation.
https://cloud.google.com/apis/docs/enable-disable-apis

### 2. Configure connections and authentications
1. Install and configure gsutil https://cloud.google.com/storage/docs/gsutil_install
2. Configure Airflow connection to GCP cloud https://airflow.apache.org/docs/apache-airflow-providers-google/stable/connections/gcp.html
3. Configure Airflow connection to SFTP server https://airflow.apache.org/docs/apache-airflow-providers-sftp/stable/connections/sftp.html
4. Configure config.json file in the root directory of the repo

### 3. Install API libraries via pip.
Create virtual env:

```python -m venv env```

For linux in bash use this:

```source env/bin/activate```

For windows in powershell use this:

```env/Scripts/Activate.ps1```

Install reqs:

```pip install -r requirements.txt'```

### 4. Run the code to check, then move it to the airflow dags repo
```python Task_BigQuery.py```

---
## Ways to improve (out of scope):
1. move logs from stdout to the local file 
2. set up bucket for logging and append every X minutes local logs
3. bigquery_to_task3 change csv to parquet, it's lack of docs to configure (https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/transfers/bigquery_to_gcs/index.html#airflow.providers.google.cloud.transfers.bigquery_to_gcs.BigQueryToGCSOperator)
4. add task.doc_md in markdown to document tasks where it would be useful
---
## Docs used:
* https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/cloud/index.html
* https://airflow.apache.org/docs/apache-airflow-providers-google/stable/operators/transfer/index.html
* https://airflow.apache.org/docs/apache-airflow-providers-google/stable/_api/airflow/providers/google/cloud/operators/gcs/index.html#airflow.providers.google.cloud.operators.gcs.GCSCreateBucketOperator 
* https://airflow.apache.org/docs/apache-airflow-providers-http/stable/_api/airflow/providers/http/sensors/http/index.html 