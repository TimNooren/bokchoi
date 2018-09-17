#!/usr/bin/env python3.6
"""
Class which can be used to deploy and run EC2 spot instances
"""

from base64 import b64encode
import os
import time

from bokchoi import utils
from bokchoi.ssh import SSH
from bokchoi.aws import common

DEFAULT_TRUST_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}"""

DEFAULT_POLICY = """{{
  "Version": "2012-10-17",
  "Statement": [
    {{
      "Action": [
        "s3:Get*",
        "s3:List*",
        "s3:Put*"
      ],
      "Effect": "Allow",
      "Resource": "arn:aws:s3:::{bucket}/*"
    }},
    {{
      "Action": [
        "logs:*"
      ],
      "Effect": "Allow",
      "Resource": "*"
    }}
  ]
}}"""


class EC2:
    """Create EC2 object which can be used to schedule jobs"""

    region = common.get_default_region()

    default_config = {
            'SpotPrice': '0.10',
            'LaunchSpecification': {
                'ImageId': '',
                'InstanceType': 'c5.xlarge',
                'SubnetId': ''
            }
    }

    def __init__(self, project_name, config):

        self.validate(config['EC2'])

        self.config = config

        self.launch_spec = config['EC2']['LaunchSpecification']
        self.subnet = common.get_subnet(self.launch_spec['SubnetId'])

        self.project_id = utils.create_project_id(project_name, common.get_aws_account_id())
        self.package_name = 'bokchoi-' + project_name + '.zip'

    def validate(self, config):

        non_optional = {'SpotPrice', 'LaunchSpecification'}
        missing_keys = non_optional - set(config)

        if missing_keys:
            raise AssertionError('Missing keys in EC2 config: {}'.format(', '.join(missing_keys)))

    def deploy(self, path):
        """Zip package and deploy to S3"""

        bucket_name = common.create_bucket(self.region, self.project_id)

        package, fingerprint = utils.zip_package(path, self.config.get('Requirements', []))
        common.upload_to_s3(bucket_name, package, self.package_name, fingerprint)

        policies = self.create_policies(self.config['EC2'].get('CustomPolicy'))

        self.create_default_role_and_profile(policies)

        common.create_security_group(self.project_id
                                     , self.project_id
                                     , self.subnet.vpc_id
                                     , {'CidrIp': utils.get_my_ip() + '/32'
                                        , 'FromPort': 22
                                        , 'ToPort': 22
                                        , 'IpProtocol': 'tcp'}
                                     )

        common.create_log_group(self.project_id)

        return 'Deployed!'

    def undeploy(self, dryrun):
        """Deletes all policies, users, and instances permanently"""

        common.cancel_spot_request(self.project_id, dryrun)

        for instance in common.get_instances(self.project_id):
            common.terminate_instance(instance, dryrun)

        common.delete_bucket(self.project_id, dryrun)

        for policy in common.get_policies(self.project_id):
            common.delete_policy(policy, dryrun)

        for instance_profile in common.get_instance_profiles(self.project_id):
            common.delete_instance_profile(instance_profile, dryrun)

        for role in common.get_roles(self.project_id):
            common.delete_role(role, dryrun)

        for group in common.get_security_groups(self.project_id):
            common.delete_security_group(group, dryrun)

        common.delete_log_group(self.project_id, dryrun)

        return 'Undeployed!'

    def run(self):
        """Create EC2 machine with given AMI and instance settings"""

        public_key = SSH(self.project_id).public_key if self.config.get('Notebook') else ''

        if self.config.get('Notebook'):
            security_group = common.get_security_groups(self.project_id, self.project_id)[0]
            if self.launch_spec.get('SecurityGroupIds'):
                self.launch_spec['SecurityGroupIds'] += [security_group.group_id]
            else:
                self.launch_spec['SecurityGroupIds'] = [security_group.group_id]

        with open(os.path.join(os.path.dirname(__file__), 'ec2-startup-script.sh'), 'r') as _file:
            startup_script = _file.read()

        user_data = startup_script.format(region=self.region
                                          , project_id=self.project_id
                                          , bucket=self.project_id
                                          , package=self.package_name
                                          , entrypoint=self.config['EntryPoint']
                                          , shutdown=self.config.get('Shutdown', True)
                                          , notebook=self.config.get('Notebook', False)
                                          , public_key=public_key)

        self.launch_spec['UserData'] = b64encode(user_data.encode('ascii')).decode('ascii')
        self.launch_spec['IamInstanceProfile'] = {'Name': self.project_id}

        common.request_spot_instances(self.project_id, self.launch_spec, self.config['EC2']['SpotPrice'])

        log_stream_name = 'bokchoi-{}'.format(int(time.time()))
        common.create_log_stream(self.project_id, log_stream_name)

        print('Writing logs to: ' + log_stream_name)

        return 'Running application'

    def create_default_role_and_profile(self, policies):
        """ Creates default role and instance profile for EC2 deployment.
        :param policies:                Policies to attach to default role
        """
        role_name = self.project_id
        common.create_role(role_name, DEFAULT_TRUST_POLICY, *policies)
        common.create_instance_profile(role_name, role_name)

    def create_policies(self, custom_policy):
        """Creates policies for EMR related tasks"""
        policies = []

        # declare default policy settings
        default_policy_name = self.project_id + '-default-policy'
        default_policy_document = DEFAULT_POLICY.format(bucket=self.project_id)
        common.create_policy(default_policy_name, default_policy_document)

        policies.append(common.get_policies(default_policy_name)[0])

        if custom_policy:
            print('Creating custom policy')

            custom_policy_name = self.project_id + '-custom-policy'
            common.create_policy(custom_policy_name, custom_policy)
            policies.append(common.get_policies(custom_policy_name)[0])

        return policies

    def connect(self, local_port, remote_port):
        """Set up port forwarding to remote server"""
        instance = common.get_instances(self.project_id)[0]
        instance_ip = instance.public_ip_address or instance.private_ip_address
        SSH(self.project_id).forward(local_port or 8888, instance_ip, remote_port or 8888, 'ubuntu')

    def stop(self, dryrun=False):
        """Stop all running instances"""
        common.cancel_spot_request(self.project_id, dryrun)

        for instance in common.get_instances(self.project_id):
            common.terminate_instance(instance, dryrun)

        return 'Instances stopped'

    def status(self):
        """Status of current deployment"""
        print('\nStatus:')
        for instance in common.get_instances(self.project_id):
            print('\t' + instance.instance_id + ' : ' + instance.state['Name'])

    def logs(self):
        """Retrieve logs of latest run if available"""

        most_recent_log_stream = common.get_most_recent_log_stream(self.project_id)

        if not most_recent_log_stream:
            return

        print('Reading logs from: ' + most_recent_log_stream)

        next_token = None

        while True:
            events, next_token = common.get_log_messages(self.project_id, most_recent_log_stream, next_token)

            for event in events:

                if 'log-termination' in event['message']:
                    return

                print(event['message'].strip('\n'))

            time.sleep(2)
