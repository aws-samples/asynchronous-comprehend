import os
import boto3
import json

COMPREHEND_EVENTBUS_ARN = os.environ.get('COMPREHEND_EVENTBUS_ARN')
COMPREHEND_DATA_ACCESS_ROLE_ARN = os.environ.get('COMPREHEND_DATA_ACCESS_ROLE_ARN')
comprehend = boto3.client('comprehend')
eventbridge = boto3.client('events')


SUCCESS_STATUSES = ['SUBMITTED', 'IN_PROGRESS', 'COMPLETED']


def lambda_handler(event, context):
    '''
    Start a comprehend job, publish the submitted job status to eventbridge
    '''
    # https://boto3.amazonaws.com/v1/documentation/api/latest/reference/services/comprehend/client/start_document_classification_job.html
    event["detail"].update({"DataAccessRoleArn": COMPREHEND_DATA_ACCESS_ROLE_ARN})
    response = comprehend.start_document_classification_job(**event["detail"])
    publish_job_status_event(
        job_id=response['JobId'], job_status=response['JobStatus'], job_arn=response['JobArn'])

    if response['JobStatus'] in SUCCESS_STATUSES:
        return {'status': 'SUCCESS', 'JobResponse': response}
    return {'status': 'FAILED', 'JobResponse': response}


def publish_job_status_event(job_id, job_status, job_arn):
    '''
    Published job status to eventbridge
    '''
    eventbridge.put_events(
        Entries=[
            {
                'Source': 'polling.comprehend',
                'DetailType': 'JobSubmitted',
                'Detail': json.dumps({
                    'job_id': job_id,
                    'job_status': job_status,
                    'job_arn': job_arn
                }),
                'EventBusName': COMPREHEND_EVENTBUS_ARN
            }
        ]
    )
    return
