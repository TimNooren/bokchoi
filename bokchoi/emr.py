#!/usr/bin/env python3.6
"""
Class which can be used to deploy and run EMR jobs
"""
import boto3
import click

from . import helper

# define S3 settings
S3_BUCKET = 'bijenkorf-personalization-layer'


class EMR(object):
    """Create EMR object which can be used to schedule jobs"""
    def __init__(self, job, package_path, instance_type, num_instances):
        self.emr_client = boto3.client('emr')
        self.s3_client = boto3.resource('s3')
        self.job_name = job
        self.package_name = helper.zip_package(package_path)
        self.type = instance_type
        self.instances = num_instances

    def deploy(self):
        """Zip package and deploy to S3 so it can be used by EMR"""
        self.s3_client.upload_file(self.package_name, S3_BUCKET, self.package_name)
        self.s3_client.upload_file('requirements.txt', S3_BUCKET, 'requirements.txt')

    def undeploy(self):
        """Deletes all policies, users, and instances permanently"""
        raise NotImplementedError("Function not yet implemented for EMR")

    def run(self, main_script):
        """Create Spark cluster and run specified job
        Returns: emr job flow creation response
        """
        s3_package_uri = 's3://{bucket}/{key}'.format(bucket=S3_BUCKET, key=self.package_name)

        return self.emr_client.run_job_flow(
            Name=self.job_name,
            LogUri=S3_BUCKET,
            ReleaseLabel='emr-5.8.0',
            Instances={
                'MasterInstanceType': self.type,
                'SlaveInstanceType': self.type,
                'InstanceCount': self.instances,
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
            },
            Applications=[
                {
                    'Name': 'Spark'
                }
            ],
            BootstrapActions=[
                {
                    'Name': 'Maximize Spark Default Config',
                    'ScriptBootstrapAction': {
                        'Path': 's3://support.elasticmapreduce/spark/maximize-spark-default-config',
                    }
                },
            ],
            Steps=[
                {
                    'Name': 'Setup Debugging',
                    'ActionOnFailure': 'TERMINATE_CLUSTER',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['state-pusher-script']
                    }
                },
                {
                    'Name': 'setup - copy files',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', s3_package_uri, '/home/hadoop/']
                    }
                },
                {
                    'Name': 'Run Spark',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', '/home/hadoop/' + main_script]
                    }
                }
            ],
            VisibleToAllUsers=True,
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole'
        )
