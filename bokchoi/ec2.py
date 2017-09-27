#!/usr/bin/env python3.6
"""
Class which can be used to deploy and run EC2 spot instances
"""
import os
import base64
import click

from . import helper


class EC2(object):
    """Create EC2 object which can be used to schedule jobs"""
    def __init__(self, project, settings):
        self.project_name = project
        self.requirements = settings.get('Requirements')
        self.region = settings.get('Region')
        self.entry_point = settings.get('EntryPoint')
        self.launch_config = settings.get('EC2')
        self.custom_policy = settings.get('CustomPolicy')

        self.project_id = helper.create_job_id(project)
        self.package_name = 'bokchoi-' + self.project_name + '.zip'

        self.schedule = settings.get('Schedule')

    def deploy(self):
        """Zip package and deploy to S3"""

        bucket = helper.create_bucket(self.region, self.project_id)

        cwd = os.getcwd()
        package = helper.zip_package(cwd, self.requirements)

        helper.upload_zip(bucket, package, self.package_name)

        policy_arns = []

        default_policy_name = self.project_id + '-default-policy'
        default_policy_document = helper.DEFAULT_POLICY.format(bucket=bucket)
        default_policy_arn = helper.create_policy(default_policy_name, default_policy_document)
        policy_arns.append(default_policy_arn)

        if self.custom_policy:
            click.echo('Creating custom policy')

            custom_policy_name = self.project_id + '-custom-policy'
            custom_policy_document = self.custom_policy
            custom_policy_arn = helper.create_policy(custom_policy_name, custom_policy_document)
            policy_arns.append(custom_policy_arn)

        role_name = self.project_id + '-default-role'
        helper.create_role(role_name, helper.TRUST_POLICY, *policy_arns)
        helper.create_instance_profile(role_name, role_name)

        if self.schedule:
            click.echo('Scheduling job using ' + self.schedule)
            helper.create_scheduler(self.project_id, self.project_name, self.schedule, self.requirements)

    def undeploy(self):
        """Deletes all policies, users, and instances permanently"""

        helper.cancel_spot_request(self.project_id)
        helper.terminate_instances(self.project_id)

        helper.delete_bucket(self.project_id)

        for policy in helper.get_policies(self.project_id):
            helper.delete_policy(policy)

        for instance_profile in helper.get_instance_profiles(self.project_id):
            helper.delete_instance_profile(instance_profile)

        for role in helper.get_roles(self.project_id):
            helper.delete_role(role)

        helper.delete_scheduler(self.project_id)

        rule_name = self.project_id + '-schedule-event'
        helper.delete_cloudwatch_rule(rule_name)

    def run(self):
        """Create EC2 machine with given AMI and instance settings"""
        click.echo("Running EC2 instance")

        bucket_name = self.project_id

        app, entry = self.entry_point.split('.')
        user_data = helper.USER_DATA.format(bucket=bucket_name, package=self.package_name, app=app, entry=entry)
        self.launch_config['LaunchSpecification']['UserData'] = base64.b64encode(user_data.encode('ascii')).decode('ascii')

        self.launch_config['LaunchSpecification']['IamInstanceProfile'] = {'Name': self.project_id + '-default-role'}

        helper.request_spot_instances(self.project_id, self.launch_config)
