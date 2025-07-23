from googleapiclient.discovery import build

def trigger_df_job(cloud_event,environment):
    service = build('dataflow','vlb3')
    project = 'proj-poc-001'
    template_path = "gs://dataflow-templates-us-central1-latest/GCS_Text_to_BigQuery"

    template_body = {
        "jobName":"bq-load",
        "parameters":{
            "javascriptTextTransformGcsPath":"gs://nj-bkt-dataflow-metadata/udf.js",
            "JSONPath":"gs://nj-bkt-dataflow-metadata/bq.json",
            "javascriptTextTransformFunctionName":"transform",
            "outputTable":"prj-poc-001:cricket_dataset.icc_odi_batsman_ranking",
            "inputFilePattern":"gs://nj-bkt-ranking-data/batsmen_ranking.csv",
            "bigQueryLoadingTemporaryDirectory":"gs://nj-bkt-dataflow-metadata",

        }
    }


    request = service.projects().templates().launch(projectId=project,gcsPath=template_path,body=template_body)
    response = request.execute()
    print(response)