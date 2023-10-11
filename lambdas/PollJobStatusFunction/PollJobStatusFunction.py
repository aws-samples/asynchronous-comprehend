import os
import boto3
import json

COMPREHEND_EVENTBUS_ARN = os.environ.get('COMPREHEND_EVENTBUS_ARN')
comprehend = boto3.client('comprehend')
eventbridge = boto3.client('events')


IN_PROGRESS_STATUSES = ['SUBMITTED', 'IN_PROGRESS']
SUCCESS_STATUSES = ['COMPLETED']
FAILED_STATUSES = ['FAILED', 'STOP_REQUESTED', 'STOPPED']


def lambda_handler(event, context):
    '''
    Describe the comprehend job
    '''
    job = event["JobResponse"]
    response = comprehend.describe_document_classification_job(
        JobId=event["JobResponse"]["JobId"])
    job["JobStatus"] = response["DocumentClassificationJobProperties"]["JobStatus"]
    job["Message"] = response["DocumentClassificationJobProperties"].get("Message", "")

    if job['JobStatus'] in IN_PROGRESS_STATUSES:
        return {'status': 'IN_PROGRESS', 'JobResponse': job}
    publish_job_status_event(
        job_id=job['JobId'], job_status=job['JobStatus'], job_arn=job['JobArn'])
    if job['JobStatus'] in SUCCESS_STATUSES:
        return {'status': 'SUCCESS', 'JobResponse': job}
    return {'status': 'FAILED', 'JobResponse': job}


def publish_job_status_event(job_id, job_status, job_arn):
    '''
    Published job status to eventbridge
    '''
    eventbridge.put_events(
        Entries=[
            {
                'Source': 'polling.comprehend',
                'DetailType': 'JobFinished',
                'Detail': json.dumps({
                    'job_id': job_id,
                    'job_status': job_status,
                    'job_arn': job_arn
                }),
                'EventBusName': COMPREHEND_EVENTBUS_ARN
            }
        ]
    )
