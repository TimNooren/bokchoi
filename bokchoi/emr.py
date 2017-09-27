#!/usr/bin/env python3.6
"""
Class which can be used to deploy and run EMR jobs
"""
import sys
import os
from botocore.exceptions import ClientError
import boto3
import click

from . import helper

class EMR(object):
    """Create EMR object which can be used to schedule jobs"""
    def __init__(self, project, settings):
        self.emr_client = boto3.client('emr')
        self.settings = settings
        self.project_name = project
        self.job_id = helper.create_job_id(project)
        self.bucket = create_bucket(settings, self.job_id)

    def deploy(self):
        """Zip package and deploy to S3 so it can be used by EMR"""

        # create bucket for files
        click.secho('Created bucket: ' + self.bucket, fg='green')

        # define requirements
        requirements = self.settings.get('Requirements', None)

        # zip package and send to s3
        zip_file = helper.zip_package(os.getcwd(), requirements)
        helper.upload_zip(self.bucket, zip_file, self.job_id + '.zip')

        # create policies and roles
        policies = create_policies(self.settings, self.job_id, self.bucket)
        create_roles(self.job_id, policies)

        # schedule if needed
        if self.settings.get('Schedule'):
            schedule(self.settings, self.job_id, self.project_name, requirements)

    def undeploy(self):
        """Deletes all policies, users, and instances permanently"""
        # terminate emr cluster (not needed if job is not persistent)
        #TODO: implement

        # remove policies and roles
        for pol in helper.get_policies(self.job_id):
            helper.delete_policy(pol)

        for role in helper.get_roles(self.job_id):
            helper.delete_role(role)

        for prof in helper.get_instance_profiles(self.job_id):
            helper.delete_instance_profile(prof)

        # remove s3 bucket
        helper.delete_bucket(self.bucket)
        helper.delete_scheduler(self.job_id)
        helper.delete_cloudwatch_rule(self.job_id + '-schedule-event')



    def run(self):
        """Create Spark cluster and run specified job
        Returns: emr job flow creation response
        """
        s3_package_uri = 's3://{bucket}/{key}'.format(bucket=self.bucket, key=self.job_id + '.zip')

        instance_type = self.settings['EMR']['LaunchSpecification']['InstanceType']
        instances = self.settings['EMR']['InstanceCount']
        main_script = self.settings['EntryPoint']

        return self.emr_client.run_job_flow(
            Name=self.job_id,
            LogUri=self.job_id,
            ReleaseLabel='emr-5.8.0',
            Instances={
                'MasterInstanceType': instance_type,
                'SlaveInstanceType': instance_type,
                'InstanceCount': instances,
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
            },
            Applications=[{'Name': 'Spark'}],
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
                    'Name': 'setup - unzip files',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['unzip', self.job_id + '.zip']
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


# TODO: refactor to policy class?
def create_policies(settings, job_id, bucket):
    """Create policies for EMR related tasks"""
    policy_arns = []

    # declare default policy settings
    default_policy_name = job_id + '-default-policy'
    default_policy_document = helper.DEFAULT_POLICY.format(bucket=bucket)
    default_policy_arn = helper.create_policy(default_policy_name, default_policy_document)
    policy_arns.append(default_policy_arn)

    if settings.get('CustomPolicy'):
        click.echo('Creating custom policy')

        custom_policy_name = job_id + '-custom-policy'
        custom_policy_document = settings['CustomPolicy']
        custom_policy_arn = helper.create_policy(custom_policy_name, custom_policy_document)
        policy_arns.append(custom_policy_arn)

    return policy_arns

# TODO: refactor to role class (together with policies)?
def create_roles(job_id, policy_arns):
    """Create roles for EMR jobs"""
    role_name = job_id + '-default-role'
    helper.create_role(role_name, helper.TRUST_POLICY, *policy_arns)
    helper.create_instance_profile(role_name, role_name)


def schedule(settings, job_id, project, requirements):
    """Schedule task"""
    task = settings['Schedule']
    click.echo('Scheduling job using ' + task)
    helper.create_scheduler(job_id, project, task, requirements)


# TODO: move to aws_utils
def create_bucket(settings, job_id):
    """Create new bucket or use existing one"""
    try:
        bucket = helper.create_bucket(settings['Region'], job_id)
    except ClientError as exception:
        if exception.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            click.secho('Bucket already exists and owned by you, continuing', fg='green')
            bucket = job_id
        else:
            click.secho('Error encountered during bucket creation: ' + str(exception), fg='red')
            sys.exit(1)

    return bucket
