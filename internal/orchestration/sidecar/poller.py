"""
Clavesa pipeline poller — invoked on a schedule by EventBridge.

Checks all source SQS queues. If any queue has messages, starts the
Step Functions state machine once. The poller does NOT consume the
queues: it reads ApproximateNumberOfMessages and leaves every message
in place for the runner Lambda to drain (ReceiveMessage + DeleteMessage
after the Delta write commits). Consuming here would steal the keys the
runner needs to know which objects to ingest.
"""
import boto3
import json
import os


def arn_to_url(arn):
    # arn:aws:sqs:region:account:queue-name -> https://sqs.region.amazonaws.com/account/queue-name
    parts = arn.split(":")
    return f"https://sqs.{parts[3]}.amazonaws.com/{parts[4]}/{parts[5]}"


def handler(event, context):
    sqs = boto3.client("sqs")
    sfn = boto3.client("stepfunctions")

    queue_arns = json.loads(os.environ["QUEUE_ARNS"])
    state_machine_arn = os.environ["STATE_MACHINE_ARN"]

    # Non-consuming depth check: ApproximateNumberOfMessages is eventually
    # consistent (can briefly read 0 right after a message lands), but it
    # leaves messages in the queue for the runner to drain. ReceiveMessage
    # would hide them from the runner during the visibility window.
    has_messages = False
    for arn in queue_arns:
        resp = sqs.get_queue_attributes(
            QueueUrl=arn_to_url(arn),
            AttributeNames=["ApproximateNumberOfMessages"],
        )
        if int(resp["Attributes"].get("ApproximateNumberOfMessages", "0")) > 0:
            has_messages = True
            break

    if not has_messages:
        return {"triggered": False}

    # Smuggle the trigger source through the SFN execution input so the
    # runs_writer Lambda can stamp `runs.trigger` without a CloudTrail
    # subscription. Same idiom as the EventBridge schedule target.
    sfn.start_execution(
        stateMachineArn=state_machine_arn,
        input=json.dumps({"_trigger": "event"}),
    )

    return {"triggered": True}
