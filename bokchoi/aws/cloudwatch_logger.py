#!/usr/bin/env python3

import os
import sys
import time

import boto3


class CloudwatchLogger:

    """Reads messages from stdin and logs them to Cloudwatch Logs"""

    def __init__(self):

        self.logs_client = boto3.client('logs', region_name=os.environ['REGION'])

        self.log_group_name = os.environ['BOKCHOI_PROJECT_ID']
        self.log_stream_name, self.sequence_token = self.get_most_recent_log_stream(self.log_group_name)

        self.stage = sys.argv[1]

    def get_most_recent_log_stream(self, log_group_name):
        """Returns most recent log stream. Should always exist since it's
        created when 'bokchoi run' is executed"""
        response = self.logs_client.describe_log_streams(
            logGroupName=log_group_name,
            orderBy='LogStreamName',
            descending=True,
            limit=1
        )

        log_stream = response['logStreams'][0]

        return log_stream['logStreamName'], log_stream['uploadSequenceToken']

    def log_message(self, message):
        """Log message to Cloudwatch log group"""
        log_info = {'logGroupName': self.log_group_name,
                    'logStreamName': self.log_stream_name,
                    'sequenceToken': self.sequence_token,
                    'logEvents': [
                        {
                            'timestamp': int(1000 * time.time()),
                            'message': '[{}]: {}'.format(self.stage, message)
                        },
                    ]}

        response = self.logs_client.put_log_events(**log_info)

        self.sequence_token = response['nextSequenceToken']

    def run(self):
        """Process incoming messages"""

        for message in sys.stdin:
            self.log_message(message)


if __name__ == '__main__':
    CloudwatchLogger().run()
