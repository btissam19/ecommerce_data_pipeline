import boto3

sns = boto3.client('sns')

def send_notification(message, subject, topic_arn):
    sns.publish(
        TopicArn=topic_arn,
        Message=message,
        Subject=subject
    )
