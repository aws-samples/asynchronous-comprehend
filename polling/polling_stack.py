from aws_cdk import (
    aws_lambda as _lambda,
    aws_iam as iam,
    aws_events as events,
    aws_events_targets as targets,
    Stack,
    aws_stepfunctions as stepfunctions,
    aws_stepfunctions_tasks as stepfunction_tasks,
    Duration,
    CfnOutput
)

from constructs import Construct


class ComprehendJobPollingStack(Stack):
    def __init__(self, scope: Construct, id: str, **kwargs) -> None:
        self.event_bus = None
        super().__init__(scope, id, **kwargs)

        # Event Bus for comprehend jobs
        self.event_bus = events.EventBus(
            self, 'ComprehendJobEventBus', event_bus_name="comprehend")
        comprehend_data_access_role = iam.Role(self, 'ComprehendDataAccessRole',
                                               assumed_by=iam.ServicePrincipal(
                                                   'comprehend.amazonaws.com'),
                                               managed_policies=[
                                                   iam.ManagedPolicy.from_aws_managed_policy_name(
                                                       'service-role/ComprehendDataAccessRolePolicy'),
                                                   iam.ManagedPolicy.from_aws_managed_policy_name(
                                                       'AmazonS3FullAccess')
                                               ]
                                               )

        # Lambda Handlers Definitions
        submit_lambda = _lambda.Function(self, 'SubmitJobLambda',
                                         handler='SubmitJobFunction.lambda_handler',
                                         runtime=_lambda.Runtime.PYTHON_3_9,
                                         code=_lambda.Code.from_asset(
                                             'lambdas/SubmitJobFunction'),
                                         environment={
                                             'COMPREHEND_EVENTBUS_ARN': self.event_bus.event_bus_arn,
                                             'COMPREHEND_DATA_ACCESS_ROLE_ARN': comprehend_data_access_role.role_arn
                                         },
                                         )

        allow_start_comprehend_jobs = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'comprehend:StartDocumentClassificationJob',
                'comprehend:TagResource',
                "textract:DetectDocumentText",  # textract required for submitting PDFs to Comprehend
                "textract:StartDocumentTextDetection",
                "textract:StartDocumentAnalysis",
                "textract:AnalyzeDocument",
                "textract:GetDocumentTextDetection",
                "textract:GetDocumentAnalysis",
                "iam:GetRole",
                "iam:PassRole",
            ],
            resources=['*']
        )

        allow_pass_role = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'iam:PassRole'
            ],
            resources=[comprehend_data_access_role.role_arn]
        )
        submit_lambda.add_to_role_policy(allow_start_comprehend_jobs)
        submit_lambda.add_to_role_policy(allow_pass_role)
        self.event_bus.grant_put_events_to(submit_lambda)

        poll_status_lambda = _lambda.Function(self, 'PollJobStatusLabda',
                                              handler='PollJobStatusFunction.lambda_handler',
                                              runtime=_lambda.Runtime.PYTHON_3_9,
                                              code=_lambda.Code.from_asset(
                                                  'lambdas/PollJobStatusFunction'),
                                              environment={
                                                  'COMPREHEND_EVENTBUS_ARN': self.event_bus.event_bus_arn
                                              }
                                              )

        allow_list_and_describe_comprehend_jobs = iam.PolicyStatement(
            effect=iam.Effect.ALLOW,
            actions=[
                'comprehend:DescribeDocumentClassificationJob',
            ],
            resources=['*']
        )
        poll_status_lambda.add_to_role_policy(
            allow_list_and_describe_comprehend_jobs)
        self.event_bus.grant_put_events_to(poll_status_lambda)

        # Step function definitions
        submit_job = stepfunction_tasks.LambdaInvoke(
            self, "Submit Job",
            lambda_function=submit_lambda,
            output_path="$.Payload",
        )

        wait_job = stepfunctions.Wait(self, "Wait 30 Seconds",
                                      time=stepfunctions.WaitTime.duration(
                                          Duration.seconds(30))
                                      )

        status_job = stepfunction_tasks.LambdaInvoke(
            self, "Get Job Status",
            lambda_function=poll_status_lambda,
            output_path="$.Payload",
        )

        # Step function statuses
        fail_job = stepfunctions.Fail(
            self, "Job Failed",
            cause='AWS Comprehend Job Failed',
            error='Invalid input parameters'
        )

        succeed_job = stepfunctions.Succeed(
            self, "Job Succeeded",
            comment='Comprehend Job Finished'
        )

        definition = submit_job.next(wait_job)\
            .next(status_job)\
            .next(stepfunctions
                  .Choice(self, 'Job Complete?')
                  .when(stepfunctions.Condition.string_equals('$.status', 'SUBMITTED'), wait_job)
                  .when(stepfunctions.Condition.string_equals('$.status', 'IN_PROGRESS'), wait_job)
                  .when(stepfunctions.Condition.string_equals('$.status', 'SUCCESS'), succeed_job)
                  .otherwise(fail_job))\

        # Create state machine
        state_machine = stepfunctions.StateMachine(
            self, "ComprehendJobOrchestrator",
            definition=definition,
            timeout=Duration.hours(24),
        )

        # Create an EventBridge rule to trigger the state machine
        events.Rule(
            self,
            'JobRequestRule',
            event_bus=self.event_bus,
            event_pattern=events.EventPattern(detail_type=["JobRequest"]),
            targets=[targets.SfnStateMachine(state_machine)]
        )
        CfnOutput(self, "ComprehendEventBusArn", value=self.event_bus.event_bus_arn)
        CfnOutput(self, "ComprehendDataAccessRoleArn", value=comprehend_data_access_role.role_arn)
