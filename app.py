#!/usr/bin/env python3
import aws_cdk as cdk

from polling.polling_stack import ComprehendJobPollingStack
from logger.event_logging_stack import EventLoggingStack



app = cdk.App()
comprehend_polling_stack = ComprehendJobPollingStack(app, "ComprehendJobPollingStack")
EventLoggingStack(app, "EventLoggingStack", event_bus=comprehend_polling_stack.event_bus)
app.synth()
