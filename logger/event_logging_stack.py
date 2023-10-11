from aws_cdk import (
    aws_lambda as _lambda,
    aws_events as events,
    aws_events_targets as targets,
    aws_logs as logs,
    Stack,
    RemovalPolicy,
    CfnOutput
)

from constructs import Construct


class EventLoggingStack(Stack):
    def __init__(self, scope: Construct, id: str, event_bus, **kwargs) -> None:
        super().__init__(scope, id, **kwargs)

        # CloudWatch Logs Group
        log_group = logs.LogGroup(
            self, "logs",
            removal_policy = RemovalPolicy.DESTROY
        )

        # EventBridge Rule
        rule = events.Rule(
            self, "rule",
            event_bus=event_bus
        )
        rule.add_event_pattern(
            source=["polling.comprehend"],
        )
        rule.add_target(targets.CloudWatchLogGroup(log_group))

        CfnOutput(
            self, "LogGroupName",
            description="Name of CloudWatch Log Group",
            value=log_group.log_group_name
        )
