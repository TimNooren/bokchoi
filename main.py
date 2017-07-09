
from time import sleep
import boto3
import json
import os
import zipfile
import hashlib
from botocore.exceptions import ClientError


session = boto3.Session(region_name='eu-west-1')
ec2 = session.client('ec2')

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
        "s3:List*"
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


def load_settings():
    with open('bokchoi_settings.json', 'r') as f_setting:
        return json.load(f_setting)


def zip_package(name):
    cwd = os.getcwd()
    zip_name = 'bokchoi-' + name + '.zip'
    rootlen = len(cwd) + 1

    with zipfile.ZipFile('\\'.join((cwd, zip_name)), 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for base, dirs, files in os.walk(cwd):
            for file in files:
                if file.endswith('.zip'):
                    continue

                fn = os.path.join(base, file)
                zip_file.write(fn, fn[rootlen:])

    return zip_name


def upload_zip(zip_file_name, bucket):
    s3 = session.resource('s3')
    s3.Bucket(bucket).upload_file(zip_file_name, zip_file_name)


def create_bucket(region, job_id):

    bucket_name = job_id

    s3 = session.resource('s3')
    s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})

    return bucket_name


def create_instance_profile(profile_name, role_name=None):
    iam = session.client('iam')

    create_instance_profile_response = iam.create_instance_profile(
        InstanceProfileName=profile_name
    )

    if role_name:
        iam.add_role_to_instance_profile(
            InstanceProfileName=profile_name,
            RoleName=role_name
        )

    return create_instance_profile_response['InstanceProfile']


def create_policy(name, document):
    iam = boto3.client('iam')

    response = iam.create_policy(
        PolicyName=name,
        PolicyDocument=document
    )

    return response['Policy']['Arn']


def create_role(role_name, trust_policy, *policy_arns):
    iam = boto3.client('iam')

    create_role_response = iam.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=trust_policy
    )
    print('Policy arns: ', *policy_arns)
    for policy_arn in policy_arns:
        attach_role_policy_response = iam.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )

        print(attach_role_policy_response)

    return create_role_response['Role']


def get_aws_account_id():
    return ec2.describe_security_groups(GroupNames=['Default'])['SecurityGroups'][0]['OwnerId']


def create_job_id(project):
    aws_account_id = get_aws_account_id()
    return 'bokchoi-' + hashlib.sha1((aws_account_id + project).encode()).hexdigest()


def request_spot_instance(job_id, settings):

    response = ec2.request_spot_instances(**settings)

    print(response)

    spot_instance_request_id = response['SpotInstanceRequests'][0]['SpotInstanceRequestId']

    ec2.get_waiter('spot_instance_request_fulfilled').wait(SpotInstanceRequestIds=[spot_instance_request_id])

    tag = ec2.create_tags(Resources=[spot_instance_request_id], Tags=[{'Key': 'bokchoi-id', 'Value': job_id}])

    response = ec2.describe_spot_instance_requests(SpotInstanceRequestIds=[spot_instance_request_id])
    instance_ids = [request['InstanceId'] for request in response['SpotInstanceRequests']]

    ec2.create_tags(Resources=instance_ids, Tags=[{'Key': 'bokchoi-id', 'Value': job_id}])

    print(tag)


def cancel_spot_request(job_id):
    print('\nCancelling spot request')
    filters = [{'Name': 'tag:bokchoi-id', 'Values': [str(job_id)]}
               , {'Name': 'state', 'Values': ['open', 'active']}]
    response = ec2.describe_spot_instance_requests(Filters=filters)

    spot_instance_request_ids = [request['SpotInstanceRequestId'] for request in response['SpotInstanceRequests']]

    try:
        ec2.cancel_spot_instance_requests(SpotInstanceRequestIds=spot_instance_request_ids)
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidParameterCombination':
            print('No spot requests to cancel')
        else:
            raise e

    print('Spot requests cancelled')


def terminate_instances(job_id):
    print('\nTerminating instances')
    filters = [{'Name': 'tag:bokchoi-id', 'Values': [str(job_id)]}]
    ec2_resource = session.resource('ec2')
    ec2_resource.instances.filter(Filters=filters).terminate()
    print('Instances terminated')


def delete_bucket(job_id):
    print('\nDelete Bucket')
    s3_resource = session.resource('s3')
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
    iam = session.resource('iam')
    for instance_profile in iam.instance_profiles.all():
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
    iam = session.resource('iam')
    for role in iam.roles.all():
        if job_id in role.role_name:
            yield role


def delete_role(role):

    role_name = role.role_name
    print('\nDeleting Role:', role_name)

    try:
        for policy in role.attached_policies.all():
            policy.detach_role(RoleName=role.role_name)
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
    iam = session.resource('iam')
    for policy in iam.policies.filter(Scope='Local'):
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


def create_lambda_scheduler(job_id, project, schedule):

    import scheduler

    cwd = os.getcwd()
    zip_name = 'bokchoi-scheduler.zip'

    with zipfile.ZipFile('\\'.join((cwd, zip_name)), 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(scheduler.__file__, 'scheduler.py')
        zip_file.write(__file__, 'main.py')
        zip_file.write('\\'.join((cwd, 'bokchoi_settings.json')), 'bokchoi_settings.json')

    bucket_name = job_id
    zip_file_name = 'bokchoi-scheduler.zip'

    upload_zip(zip_file_name, bucket_name)

    policy_document = SCHEDULER_POLICY
    policy_name = job_id + '-scheduler-policy'
    policy_arn = create_policy(policy_name, policy_document)

    role_name = job_id + '-scheduler-role'
    response = create_role(role_name, SCHEDULER_TRUST_POLICY, policy_arn)

    lambda_client = session.client('lambda')
    print(schedule)
    sleep(40)

    # AWS has some specific demands on cron schedule:
    # http://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
    create_function_response = lambda_client.create_function(FunctionName=job_id + '-scheduler'
                                                             , Runtime='python3.6'
                                                             , Handler='scheduler.run'
                                                             , Role=response['Arn']
                                                             , Code={'S3Bucket': bucket_name, 'S3Key': zip_file_name}
                                                             , Timeout=30
                                                             , Environment={'Variables': {'project': project}}
                                                             , Tags={'bokchoi-id': job_id})

    events = boto3.client('events')

    policy_document = EVENT_POLICY
    policy_name = job_id + '-event-policy'
    policy_arn = create_policy(policy_name, policy_document)

    role_name = job_id + '-event-role'
    response = create_role(role_name, EVENT_TRUST_POLICY, policy_arn)

    sleep(40)

    rule_name = job_id + '-schedule-event'
    events.put_rule(Name=rule_name
                    , ScheduleExpression='cron({})'.format(schedule)
                    , State='ENABLED'
                    , RoleArn=response['Arn'])

    events.put_targets(Rule=rule_name
                       , Targets=[{'Arn': create_function_response['FunctionArn'], 'Id': '0'}])

    response = lambda_client.add_permission(
        FunctionName=job_id + '-scheduler',
        StatementId='0',
        Action='lambda:InvokeFunction',
        Principal='events.amazonaws.com'
    )


def delete_scheduler_lambda(job_id):
    lambda_client = boto3.client('lambda')

    try:
        lambda_client.delete_function(FunctionName=job_id + '-scheduler')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print('Function does not exist')
        else:
            raise e


def delete_cloudwatch_rule(rule_name):

    events = boto3.client('events')

    try:
        events.remove_targets(Rule=rule_name
                              , Ids=['0'])
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print('No targets to remove from rule')
        else:
            raise e

    try:
        events.delete_rule(Name=rule_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print('Cloudwatch rule does not exist')
        else:
            raise e
