#!/usr/bin/env python3

import aws_cdk as cdk

from s3_events.s3_events_stack import S3EventsStack


app = cdk.App()
S3EventsStack(app, "S3EventsStack")

app.synth()
