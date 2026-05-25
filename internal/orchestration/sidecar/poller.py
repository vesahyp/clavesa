"""
Clavesa pipeline poller — invoked on a schedule by EventBridge.

Checks all source SQS queues. If any queue has messages, starts the
Step Functions state machine once and purges all queues.
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

    # receive_message (not get_queue_attributes): ApproximateNumberOfMessages
    # is eventually consistent and can read 0 right after a message lands.
    # VisibilityTimeout=0 + no delete; purge_queue below handles cleanup.
    has_messages = False
    for arn in queue_arns:
        resp = sqs.receive_message(
            QueueUrl=arn_to_url(arn),
            MaxNumberOfMessages=1,
            VisibilityTimeout=0,
            WaitTimeSeconds=0,
        )
        if resp.get("Messages"):
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

    for arn in queue_arns:
        try:
            sqs.purge_queue(QueueUrl=arn_to_url(arn))
        except sqs.exceptions.PurgeQueueInProgress:
            pass  # already being purged, fine

    return {"triggered": True}
