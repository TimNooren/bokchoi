
import boto3
import click
import json
import os
import zipfile
import hashlib
from botocore.exceptions import ClientError


session = boto3.Session(region_name='eu-west-1')
ec2 = session.client('ec2')

USER_DATA = """#!/bin/bash
sudo curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip"
python3 -c "import zipfile; zf = zipfile.ZipFile('/awscli-bundle.zip'); zf.extractall('/');"
sudo chmod u+x /awscli-bundle/install
python3 /awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws
aws s3 cp s3://{bucket}/{package} /tmp/
cd /tmp
python3 -c "import zipfile; zf = zipfile.ZipFile('/tmp/{package}'); zf.extractall('/tmp/');"
python3 -c "import {app}; {app}.{entry}();"
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

IAM_PROFILE = """{{
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
        "Service": "ec2.amazonaws.com"
      },
      "Action": "sts:AssumeRole"
    }
  ]
}"""


def load_settings():
    with open('buzz_settings.json', 'r') as f_setting:
        return json.load(f_setting)


def zip_package(name):
    cwd = os.getcwd()
    zip_name = 'buzz-' + name + '.zip'
    rootlen = len(cwd) + 1

    with zipfile.ZipFile('\\'.join((cwd, zip_name)), 'w', zipfile.ZIP_DEFLATED) as zip_file:
        for base, dirs, files in os.walk(cwd):
            for file in files:
                fn = os.path.join(base, file)
                zip_file.write(fn, fn[rootlen:])

    return zip_name


def upload_zip(zip_file_name, bucket):
    s3 = session.resource('s3')
    s3.Bucket(bucket).upload_file(zip_file_name, zip_file_name)


def create_bucket(region, job_id):

    bucket_name = job_id

    s3 = session.resource('s3')
    response = s3.create_bucket(Bucket=bucket_name, CreateBucketConfiguration={'LocationConstraint': region})
    click.echo(response)
    return bucket_name


def create_role(job_id, bucket):
    iam = boto3.client('iam')

    role_name = job_id + '-default-role'
    try:
        create_role_response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=TRUST_POLICY
        )
    except ClientError as e:
        click.echo('Role already exists')

    create_policy_response = iam.create_policy(
        PolicyName=job_id + '-default-policy',
        PolicyDocument=IAM_PROFILE.format(bucket=bucket)
    )

    attach_role_policy_response = iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn=create_policy_response['Policy']['Arn']
    )

    create_instance_profile_response = iam.create_instance_profile(
        InstanceProfileName=role_name
    )

    add_role_to_instance_profile_response = iam.add_role_to_instance_profile(
        InstanceProfileName=role_name,
        RoleName=role_name
    )

    return create_instance_profile_response['InstanceProfile']['Arn']


def get_aws_account_id():
    return ec2.describe_security_groups(GroupNames=['Default'])['SecurityGroups'][0]['OwnerId']


def create_job_id(project):
    aws_account_id = get_aws_account_id()
    return 'buzz-' + hashlib.sha1((aws_account_id + project).encode()).hexdigest()


def request_spot_instance(job_id, settings):

    response = ec2.request_spot_instances(**settings)

    click.echo(response)

    spot_instance_request_id = response['SpotInstanceRequests'][0]['SpotInstanceRequestId']

    ec2.get_waiter('spot_instance_request_fulfilled').wait(SpotInstanceRequestIds=[spot_instance_request_id])

    tag = ec2.create_tags(Resources=[spot_instance_request_id], Tags=[{'Key': 'buzz-id', 'Value': job_id}])

    response = ec2.describe_spot_instance_requests(SpotInstanceRequestIds=[spot_instance_request_id])
    instance_ids = [request['InstanceId'] for request in response['SpotInstanceRequests']]

    ec2.create_tags(Resources=instance_ids, Tags=[{'Key': 'buzz-id', 'Value': job_id}])

    click.echo(tag)


def cancel_spot_request(job_id):
    click.echo('\nCancelling spot request')
    filters = [{'Name': 'tag:fleet-id', 'Values': [str(job_id)]}
               , {'Name': 'state', 'Values': ['open', 'active']}]
    response = ec2.describe_spot_instance_requests(Filters=filters)

    spot_instance_request_ids = [request['SpotInstanceRequestId'] for request in response['SpotInstanceRequests']]

    try:
        ec2.cancel_spot_instance_requests(SpotInstanceRequestIds=spot_instance_request_ids)
    except ClientError as e:
        if e.response['Error']['Code'] == 'InvalidParameterCombination':
            click.echo('No spot requests to cancel')
        else:
            raise e

    click.echo('Spot requests cancelled')


def terminate_instances(job_id):
    click.echo('\nTerminating instances')
    filters = [{'Name': 'tag:fleet-id', 'Values': [str(job_id)]}]
    ec2_resource = session.resource('ec2')
    ec2_resource.instances.filter(Filters=filters).terminate()
    click.echo('Instances terminated')


def delete_bucket(job_id):
    click.echo('\nDelete Bucket')
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


def delete_instance_profile(job_id):
    click.echo('\nDelete Instance Profile')
    iam_resource = session.resource('iam')
    instance_profile_name = job_id + '-default-role'
    instance_profile = iam_resource.InstanceProfile(instance_profile_name)
    role_name = instance_profile_name

    try:
        instance_profile.remove_role(RoleName=role_name)
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


def delete_role(role_name):
    click.echo('\nDelete Role')
    iam_resource = session.resource('iam')
    role = iam_resource.Role(role_name)

    try:
        role.delete()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            click.echo('Role does not exist')
        else:
            raise e


def delete_policy(job_id):
    click.echo('\nDelete Policy')
    aws_account_id = get_aws_account_id()
    arn = 'arn:aws:iam::{aws_account_id}:policy/{job_id}-default-policy'.format(aws_account_id=aws_account_id
                                                                                , job_id=job_id)

    iam_resource = session.resource('iam')
    default_policy = iam_resource.Policy(arn)
    role_name = job_id + '-default-role'

    try:
        default_policy.detach_role(RoleName=role_name)
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            click.echo('Role for policy does not exist')
        else:
            raise e

    try:
        default_policy.delete()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchEntity':
            click.echo('Policy does not exist')
        else:
            raise e


def create_lambda_scheduler(job_id, project, schedule):

    import scheduler

    cwd = os.getcwd()
    zip_name = 'buzz-scheduler.zip'

    with zipfile.ZipFile('\\'.join((cwd, zip_name)), 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(scheduler.__file__, 'scheduler.py')
        zip_file.write(__file__, 'main.py')
        zip_file.write('\\'.join((cwd, 'buzz_settings.json')), 'buzz_settings.json')

    bucket_name = job_id
    zip_file_name = 'buzz-scheduler.zip'

    upload_zip(zip_file_name, bucket_name)

    iam = boto3.client('iam')

    role_name = job_id + '-scheduler-role'
    try:
        create_role_response = iam.create_role(
            RoleName=role_name,
            AssumeRolePolicyDocument=SCHEDULER_TRUST_POLICY
        )
    except ClientError as e:
        click.echo('Role already exists')

    attach_role_policy_response = iam.attach_role_policy(
        RoleName=role_name,
        PolicyArn='arn:aws:iam::aws:policy/service-role/AmazonEC2SpotFleetRole'
    )

    lambda_client = session.client('lambda')

    lambda_client.create_function(FunctionName=job_id + '-scheduler'
                                  , Runtime='python3.6'
                                  , Handler='scheduler.run'
                                  , Role=''
                                  , Code={'S3Bucket': bucket_name, 'S3Key': zip_file_name}
                                  , Timeout=30
                                  , Environment={'Variables': {'project': project}}
                                  , Tags={'buzz-id': job_id})


def delete_scheduler_lambda(job_id):
    lambda_client = boto3.client('lambda')

    try:
        lambda_client.delete_function(FunctionName=job_id + '-scheduler')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            click.echo('Function does not exist')
        else:
            raise e
