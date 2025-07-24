from googleapiclient.discovery import build
from datetime import datetime

def trigger_df_job(cloud_event, environment):
    service = build('dataflow', 'v1b3')
    project = 'endless-theorem-465911-v8'
    region = 'us-central1'
    template_path = "gs://dataflow-templates-us-central1-latest/GCS_Text_to_BigQuery"

    # Generate a unique job name to avoid conflicts
    job_name = f"bq-load-{datetime.utcnow().strftime('%Y%m%d-%H%M%S')}"

    template_body = {
        "jobName": job_name,
        "parameters": {
            "javascriptTextTransformGcsPath": "gs://nj-bkt-dataflow-metadata/udf.js",
            "JSONPath": "gs://nj-bkt-dataflow-metadata/bq.json",
            "javascriptTextTransformFunctionName": "transform",
            "outputTable": "endless-theorem-465911-v8.cricketdataset.icc_odi_batsman_ranking",
            "inputFilePattern": "gs://nj-bkt-ranking-data/batsmen_ranking.csv",
            "bigQueryLoadingTemporaryDirectory": "gs://nj-bkt-dataflow-metadata"
        }
    }

    try:
        request = service.projects().locations().templates().launch(
            projectId=project,
            location=region,
            gcsPath=template_path,
            body=template_body
        )
        response = request.execute()
        print("Dataflow job submitted successfully:")
        print(response)
    except Exception as e:
        print("Error launching Dataflow job:", e)