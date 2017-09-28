
from time import sleep
import hashlib
from io import BytesIO
import json
import os
import zipfile

import boto3
from botocore.exceptions import ClientError

session = boto3.Session()

ec2_client = session.client('ec2')
ec2_resource = session.resource('ec2')

iam_client = session.client('iam')
iam_resource = session.resource('iam')

s3_client = session.client('s3')
s3_resource = session.resource('s3')

lambda_client = session.client('lambda')

events_client = session.client('events')

USER_DATA = """#!/bin/bash

# Install aws-cli
sudo curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip"
python3 -c "import zipfile; zf = zipfile.ZipFile('/awscli-bundle.zip'); zf.extractall('/');"
sudo chmod u+x /awscli-bundle/install
python3 /awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws

# Download project zip
aws s3 cp s3://{bucket}/{package} /tmp/
python3 -c "import zipfile; zf = zipfile.ZipFile('/tmp/{package}'); zf.extractall('/tmp/');"

# Install pip3 and install requirements.txt from project zip if included
curl -sS https://bootstrap.pypa.io/get-pip.py | sudo python3
[ -f /tmp/requirements.txt ] && pip3 install -r /tmp/requirements.txt

# Run app
cd /tmp
python3 -c "import {app}; {app}.{entry}();"
aws s3 cp /var/log/cloud-init-output.log s3://{bucket}/cloud-init-output.log
shutdown -h now
"""

TRUST_POLICY = """{
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
    }}
  ]
}}"""

SCHEDULER_TRUST_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "lambda.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}"""


SCHEDULER_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [{
    "Effect": "Allow",
    "Action": [
       "ec2:DescribeImages",
       "ec2:DescribeSubnets",
       "ec2:RequestSpotInstances",
       "ec2:TerminateInstances",
       "ec2:DescribeInstanceStatus",
       "ec2:DescribeSecurityGroups",
       "ec2:DescribeSpotInstanceRequests",
       "ec2:CreateTags",
       "iam:PassRole"
        ],
    "Resource": ["*"]
  }]
}"""

EVENT_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Effect": "Allow",
      "Action": [
        "lambda:InvokeFunction"
      ],
      "Resource": "*"
    }
  ]
}"""

EVENT_TRUST_POLICY = """{
  "Version": "2012-10-17",
  "Statement": [
    {
      "Sid": "",
      "Effect": "Allow",
      "Principal": {
        "Service": "events.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}"""


def retry(func, **kwargs):

    for _ in range(60):
        try:
            response = func(**kwargs)
            return response
        except ClientError as e:
            sleep(1)

    raise e


def load_settings(project_name):
    with open('bokchoi_settings.json', 'r') as settings_file:

        settings = json.load(settings_file)

        try:
            return settings[project_name]
        except KeyError:
            raise KeyError('No config found for {} in bokchoi_settings'.format(project_name))


def zip_package(path, requirements=None):

    file_object = BytesIO()

    rootlen = len(path) + 1

    with zipfile.ZipFile(file_object, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for base, _, files in os.walk(path):
            for file_name in files:
                fn = os.path.join(base, file_name)
                zip_file.write(fn, fn[rootlen:])

        if requirements:
            zip_file.writestr('requirements.txt', '\n'.join(requirements))

    file_object.seek(0)

    return file_object


def upload_zip(bucket, zip_file, zip_file_name):
    s3_resource.Bucket(bucket).put_object(Body=zip_file, Key=zip_file_name)


def create_bucket(region, bucket_name):

    try:
        s3_resource.create_bucket(Bucket=bucket_name
                                  , CreateBucketConfiguration={'LocationConstraint': region})
    except ClientError as exception:
        if exception.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print('Bucket already exists and owned by you, continuing')
        else:
            raise exception


def create_instance_profile(profile_name, role_name=None):

    create_instance_profile_response = iam_client.create_instance_profile(
        InstanceProfileName=profile_name
    )

    if role_name:
        iam_client.add_role_to_instance_profile(
            InstanceProfileName=profile_name,
            RoleName=role_name
        )

    return create_instance_profile_response['InstanceProfile']


def create_policy(name, document):

    response = iam_client.create_policy(
        PolicyName=name,
        PolicyDocument=document
    )

    return response['Policy']['Arn']


def create_role(role_name, trust_policy, *policy_arns):

    create_role_response = iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=trust_policy
    )

    for policy_arn in policy_arns:
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )

    return create_role_response['Role']


def get_aws_account_id():
    response = ec2_client.describe_security_groups(GroupNames=['Default'])
    return response['SecurityGroups'][0]['OwnerId']


def create_job_id(project):
    aws_account_id = get_aws_account_id()
    return 'bokchoi-' + hashlib.sha1((aws_account_id + project).encode()).hexdigest()


def request_spot_instances(job_id, settings):

    response = ec2_client.request_spot_instances(**settings)

    spot_request_id = response['SpotInstanceRequests'][0]['SpotInstanceRequestId']

    waiter = ec2_client.get_waiter('spot_instance_request_fulfilled')
    waiter.wait(SpotInstanceRequestIds=[spot_request_id])

    ec2_client.create_tags(Resources=[spot_request_id]
                           , Tags=[{'Key': 'bokchoi-id', 'Value': job_id}])

    response = ec2_client.describe_spot_instance_requests(SpotInstanceRequestIds=[spot_request_id])
    instance_ids = [request['InstanceId'] for request in response['SpotInstanceRequests']]

    ec2_client.create_tags(Resources=instance_ids, Tags=[{'Key': 'bokchoi-id', 'Value': job_id}])


def cancel_spot_request(job_id):
    print('\nCancelling spot request')
    filters = [{'Name': 'tag:bokchoi-id', 'Values': [str(job_id)]}
               , {'Name': 'state', 'Values': ['open', 'active']}]
    response = ec2_client.describe_spot_instance_requests(Filters=filters)

    spot_request_ids = [request['SpotInstanceRequestId'] for request in response['SpotInstanceRequests']]

    try:
        ec2_client.cancel_spot_instance_requests(SpotInstanceRequestIds=spot_request_ids)
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidParameterCombination':
            print('No spot requests to cancel')
        else:
            raise e

    print('Spot requests cancelled')


def terminate_instances(job_id):
    print('\nTerminating instances')
    filters = [{'Name': 'tag:bokchoi-id', 'Values': [str(job_id)]}]
    ec2_resource.instances.filter(Filters=filters).terminate()
    print('Instances terminated')


def delete_bucket(job_id):
    print('\nDelete Bucket')

    bucket = s3_resource.Bucket(job_id)

    try:
        bucket.objects.delete()
        bucket.delete()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            pass
        else:
            raise e


def get_instance_profiles(job_id):
    for instance_profile in iam_resource.instance_profiles.all():
        if job_id in instance_profile.instance_profile_name:
            yield instance_profile


def delete_instance_profile(instance_profile):
    instance_profile_name = instance_profile.instance_profile_name
    print('\nDeleting Instance Profile:', instance_profile_name)

    try:
        for role in instance_profile.roles_attribute:
            instance_profile.remove_role(RoleName=role['RoleName'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            pass
        else:
            raise e

    try:
        instance_profile.delete()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            pass
        else:
            raise e

    print('Successfully deleted Instance Profile:', instance_profile_name)


def get_roles(job_id):
    for role in iam_resource.roles.all():
        if job_id in role.role_name:
            yield role


def delete_role(role):

    role_name = role.role_name
    print('\nDeleting Role:', role_name)

    try:
        for policy in role.attached_policies.all():
            policy.detach_role(RoleName=role_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print('No policies to detach')
        else:
            raise e

    try:
        role.delete()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print('Role does not exist')
        else:
            raise e

    print('Successfully deleted role:', role_name)


def get_policies(job_id):
    for policy in iam_resource.policies.filter(Scope='Local'):
        if job_id in policy.policy_name:
            yield policy


def delete_policy(policy):

    policy_name = policy.policy_name
    print('\nDeleting Policy:', policy_name)

    try:
        for role in policy.attached_roles.all():
            policy.detach_role(RoleName=role.role_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print('Role for policy does not exist')
        else:
            raise e

    try:
        policy.delete()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            print('Policy does not exist')
        else:
            raise e

    print('Successfully deleted Policy:', policy_name)


def create_scheduler(job_id, project, schedule, requirements=None):

    from . import scheduler

    file_object = BytesIO()

    cwd = os.getcwd()

    with zipfile.ZipFile(file_object, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(scheduler.__file__, 'scheduler.py')
        zip_file.write(__file__, 'main.py')
        zip_file.write('\\'.join((cwd, 'bokchoi_settings.json')), 'bokchoi_settings.json')

        if requirements:
            zip_file.writestr('requirements.txt', '\n'.join(requirements))

    file_object.seek(0)

    bucket_name = job_id

    zip_file_name = 'bokchoi-scheduler.zip'
    upload_zip(bucket_name, file_object, zip_file_name)

    policy_document = SCHEDULER_POLICY
    policy_name = job_id + '-scheduler-policy'
    policy_arn = create_policy(policy_name, policy_document)

    role_name = job_id + '-scheduler-role'
    response = create_role(role_name, SCHEDULER_TRUST_POLICY, policy_arn)

    # AWS has some specific demands on cron schedule:
    # http://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
    create_function_response = retry(lambda_client.create_function
                                     , FunctionName=job_id + '-scheduler'
                                     , Runtime='python3.6'
                                     , Handler='scheduler.run'
                                     , Role=response['Arn']
                                     , Code={'S3Bucket': bucket_name
                                             , 'S3Key': zip_file_name}
                                     , Timeout=30
                                     , Environment={'Variables': {'project': project}}
                                     , Tags={'bokchoi-id': job_id})

    policy_document = EVENT_POLICY
    policy_name = job_id + '-event-policy'
    policy_arn = create_policy(policy_name, policy_document)

    role_name = job_id + '-event-role'
    response = create_role(role_name, EVENT_TRUST_POLICY, policy_arn)

    rule_name = job_id + '-schedule-event'
    retry(events_client.put_rule
          , Name=rule_name
          , ScheduleExpression=schedule
          , State='ENABLED'
          , RoleArn=response['Arn'])

    events_client.put_targets(Rule=rule_name
                              , Targets=[{'Arn': create_function_response['FunctionArn'], 'Id': '0'}])

    lambda_client.add_permission(
        FunctionName=job_id + '-scheduler',
        StatementId='0',
        Action='lambda:InvokeFunction',
        Principal='events.amazonaws.com'
    )


def delete_scheduler(job_id):

    try:
        lambda_client.delete_function(FunctionName=job_id + '-scheduler')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print('Function does not exist')
        else:
            raise e


def delete_cloudwatch_rule(rule_name):

    try:
        events_client.remove_targets(Rule=rule_name
                                     , Ids=['0'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print('No targets to remove from rule')
        else:
            raise e

    try:
        events_client.delete_rule(Name=rule_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print('Cloudwatch rule does not exist')
        else:
            raise e
