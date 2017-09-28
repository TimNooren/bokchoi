#!/usr/bin/env python3.6
"""
Class which can be used to deploy and run EMR jobs
"""

import os
import sys
import time
import boto3
import click

from . import helper


class EMR(object):
    """Create EMR object which can be used to schedule jobs"""
    def __init__(self, project, settings):
        self.settings = settings
        self.project_name = project
        self.job_id = helper.create_job_id(project)
        self.job_flow_id = None

    def create_bucket(self):
        """Create bucket which will be used by Spark"""
        # create bucket for files
        bucket_name = self.job_id
        helper.create_bucket(self.settings['Region'], bucket_name)
        click.secho('Created bucket: ' + bucket_name, fg='green')

    def send_zip_to_s3(self):
        """Package script and requirements and send file to s3"""
        requirements = self.settings.get('Requirements', None)
        zip_file = helper.zip_package(os.getcwd(), requirements)
        helper.upload_zip(self.job_id, zip_file, self.job_id + '.zip')

    def create_policies(self):
        """Create policies for EMR related tasks"""
        policy_arns = []

        # declare default policy settings
        default_policy_name = self.job_id + '-default-policy'
        default_policy_document = helper.DEFAULT_POLICY.format(bucket=self.job_id)
        default_policy_arn = helper.create_policy(default_policy_name, default_policy_document)
        policy_arns.append(default_policy_arn)

        if self.settings.get('CustomPolicy'):
            click.echo('Creating custom policy')

            custom_policy_name = self.job_id + '-custom-policy'
            custom_policy_document = self.settings['CustomPolicy']
            custom_policy_arn = helper.create_policy(custom_policy_name, custom_policy_document)
            policy_arns.append(custom_policy_arn)

        return policy_arns

    def schedule(self):
        """Schedule task"""
        if self.settings.get('Schedule'):
            requirements = self.settings.get('Requirements', None)
            task = self.settings['Schedule']
            click.echo('Scheduling job using ' + task)
            helper.create_scheduler(self.job_id, self.project_name, task, requirements)

    def create_roles(self, policy_arns):
        """Create roles for EMR jobs"""
        role_name = self.job_id + '-default-role'
        helper.create_role(role_name, helper.TRUST_POLICY, *policy_arns)
        helper.create_instance_profile(role_name, role_name)

    def deploy(self):
        """Zip package and deploy to S3 so it can be used by EMR"""
        self.create_bucket()
        self.send_zip_to_s3()
        policies = self.create_policies()
        self.create_roles(policies)
        self.schedule()

    def run(self):
        """Create Spark cluster and run specified job"""
        emr_client = boto3.client('emr')
        self.start_spark_cluster(emr_client)
        self.step_prepare_env(emr_client)
        self.step_spark_submit(emr_client)

    def undeploy(self):
        """Deletes all policies, users, and instances permanently"""
        for pol in helper.get_policies(self.job_id):
            helper.delete_policy(pol)

        for prof in helper.get_instance_profiles(self.job_id):
            helper.delete_instance_profile(prof)

        for role in helper.get_roles(self.job_id):
            helper.delete_role(role)

        # remove s3 bucket
        helper.delete_bucket(self.job_id)
        helper.delete_scheduler(self.job_id)
        helper.delete_cloudwatch_rule(self.job_id + '-schedule-event')

    def start_spark_cluster(self, emr_client):
        """
        Start Spark cluster based on configuration given in settings
        """
        instance_type = self.settings['EMR']['LaunchSpecification']['InstanceType']
        instances = self.settings['EMR']['InstanceCount']
        ec2_key_name = self.settings['EMR']['EC2KeyName']
        version = self.settings['EMR']['Version']
        spot_price = self.settings['EMR']['SpotPrice']

        response = emr_client.run_job_flow(
            Name=self.job_id,
            LogUri="s3://{}/spark/".format(self.job_id),
            ReleaseLabel=version,
            Instances={
                'KeepJobFlowAliveWhenNoSteps': False,
                'TerminationProtected': False,
                'InstanceGroups': [
                    {
                        'Name': 'EmrMaster',
                        'Market': 'SPOT',
                        'InstanceRole': 'MASTER',
                        'BidPrice': spot_price,
                        'InstanceType': instance_type,
                        'InstanceCount': 1,
                    },
                    {
                        'Name': 'EmrCore',
                        'Market': 'SPOT',
                        'InstanceRole': 'CORE',
                        'BidPrice': spot_price,
                        'InstanceType': instance_type,
                        'InstanceCount': instances - 1,
                    },
                ],
                'Ec2KeyName': ec2_key_name
            },
            Applications=[{'Name': 'Hadoop'}, {'Name': 'Spark'}],
            JobFlowRole='EMR_EC2_DefaultRole',
            ServiceRole='EMR_DefaultRole',
            VisibleToAllUsers=True,

            BootstrapActions=[
                {
                    'Name': 'Maximize Spark Default Config',
                    'ScriptBootstrapAction': {
                        'Path': 's3://support.elasticmapreduce/spark/maximize-spark-default-config',
                    }
                },
            ],
        )
        # parse EMR response to check if successful
        response_code = response['ResponseMetadata']['HTTPStatusCode']
        if response['ResponseMetadata']['HTTPStatusCode'] == 200:
            self.job_flow_id = response['JobFlowId']
        else:
            click.secho("Error creating: (status code {})".format(response_code), fg='red')
            sys.exit(1)

        click.secho("Created Spark {} with job {}".format(version, self.job_flow_id), fg='green')

    def step_prepare_env(self, emr_client):
        """Copies files from S3 and unzips them"""
        s3_package_uri = 's3://{bucket}/{key}'.format(bucket=self.job_id, key=self.job_id + '.zip')
        root_dir = '/home/hadoop/'

        emr_client.add_job_flow_steps(
            JobFlowId=self.job_flow_id,
            Steps=[
                {
                    'Name': 'setup - copy files',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['aws', 's3', 'cp', s3_package_uri, root_dir]
                    }
                },
                {
                    'Name': 'setup - unzip files',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['unzip', root_dir + self.job_id + '.zip', '-d', root_dir]
                    }
                },
                {
                    'Name': 'setup - install python dependencies',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['sudo', 'pip-3.4', 'install', '-r', root_dir + 'requirements.txt']
                    }
                }
            ]
        )

    def step_spark_submit(self, emr_client):
        """Submit spark job given by user"""
        py3_env = 'PYSPARK_PYTHON=/usr/bin/python3'
        script = self.settings['EntryPoint']

        emr_client.add_job_flow_steps(
            JobFlowId=self.job_flow_id,
            Steps=[
                {
                    'Name': 'Run Spark',
                    'ActionOnFailure': 'CANCEL_AND_WAIT',
                    'HadoopJarStep': {
                        'Jar': 'command-runner.jar',
                        'Args': ['spark-submit', '--conf', py3_env, '/home/hadoop/' + script]
                    }
                }
            ]
        )
        click.secho("Added step 'spark-submit'", fg='green')
        time.sleep(1) # Prevent ThrottlingException
