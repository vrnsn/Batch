import boto3
import json
import os
import logging

def submit_job_landing(batch):

    response = batch.submit_job(
        jobName="symbolops-land",
        jobQueue="symbolops-highp",
        jobDefinition="symbolops",
        containerOverrides={
            "command": ["python3", "symbol_landing.py"],
            "environment": [
                {"name": "SOURCE_BUCKET_NAME", "value": "sevren-batch-source"},
                {"name": "BUCKET_PREFIX", "value": "nasdaq"}
            ]
        }
    )
    print("Job ID of {0} is {1}".format(response["jobName"], response["jobId"]))

    submit_job_staging(batch, response["jobId"])

def submit_job_staging(batch, landing_jobId):

    response = batch.submit_job(
        jobName="symbolops-stage",
        jobQueue="symbolops-highp",
        jobDefinition="symbolops",
        containerOverrides={
            "command": ["python3", "symbol_staging.py"],
            "environment": [
                {"name": "BUCKET_PREFIX", "value": "nasdaq/N"}
            ]
        },
        dependsOn=[
            {
                "jobId": landing_jobId
            }
        ]
    )
    print("Job ID of {0} is {1}. This job depends on {2}".format(response["jobName"], response["jobId"], landing_jobId))

    submit_job_processing(batch, response["jobId"])

def submit_job_processing(batch, staging_jobId):

    response = batch.submit_job(
        jobName="symbolops-process",
        jobQueue="symbolops-highp",
        jobDefinition="symbolops",
        containerOverrides={
            "command": ["python3", "symbol_processing.py"]
        },
        dependsOn=[
            {
                "jobId": staging_jobId
            }
        ]
    )

    print("Job ID of {0} is {1}. This job depends on {2}".format(response["jobName"], response["jobId"], staging_jobId))



def main():
    logging.getLogger().setLevel(logging.INFO)
    try:
        batch = boto3.client('batch', region_name='us-east-2', endpoint_url="https://batch.us-east-2.amazonaws.com")
        submit_job_landing(batch)

    except Exception:
        logging.exception("Problem with the main function", exc_info=True)

if __name__ == '__main__':
    main()