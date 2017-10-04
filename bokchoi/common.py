
from time import sleep
from io import BytesIO
import zipfile
import os
import json
import hashlib


from bokchoi import ec2
from bokchoi import emr

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
       "iam:PassRole",
       "elasticmapreduce:RunJobFlow",
       "elasticmapreduce:AddJobFlowSteps"
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


def get_aws_account_id():
    """ Returns AWS account ID"""
    response = ec2_client.describe_security_groups(GroupNames=['Default'])
    return response['SecurityGroups'][0]['OwnerId']


def create_bucket(region, bucket_name):
    """ Creates bucket to store application packages
    :param region:                  Region to create bucket in
    :param bucket_name:             Name of bucket
    :return:                        Name of bucket
    """
    try:
        s3_resource.create_bucket(Bucket=bucket_name
                                  , CreateBucketConfiguration={'LocationConstraint': region})
    except ClientError as exception:
        if exception.response['Error']['Code'] == 'BucketAlreadyOwnedByYou':
            print('Bucket already exists and owned by you, continuing')
        else:
            raise exception
    else:
        print('Created bucket: ' + bucket_name)

    return bucket_name


def upload_zip(bucket, zip_file, zip_file_name):
    """ Uploads zip file to S3
    :param bucket:                  Bucket name
    :param zip_file:                Zipped file
    :param zip_file_name:           Name of zip file in S3
    """
    s3_resource.Bucket(bucket).put_object(Body=zip_file, Key=zip_file_name)


def retry(func, **kwargs):
    """ Retries boto3 function call in case a ClientError occurs
    :param func:                    Function to call
    :param kwargs:                  Parameters to pass to function
    :return:                        Function response
    """
    for _ in range(60):
        try:
            response = func(**kwargs)
            return response
        except ClientError as e:
            sleep(1)

    raise e


def create_instance_profile(profile_name, role_name=None):
    """ Creates IAM instance profile
    :param profile_name:            Name of profile to be created
    :param role_name:               Name of role to attach to instance profile
    :return:                        API response
    """
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
    """ Creates IAM policy
    :param name:                    Name of policy to create
    :param document:                Policy document associated with policy
    :return:                        Policy ARN
    """
    try:
        response = iam_client.create_policy(
            PolicyName=name,
            PolicyDocument=document
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print('Policy already exists')
        else:
            raise e
    else:
        return response['Policy']['Arn']


def create_role(role_name, trust_policy, *policy_arns):
    """ Creates IAM role
    :param role_name:               Name of role to create
    :param trust_policy:            Trust policy to associate with role
    :param policy_arns:             ARN(s) of 1 or more policies to attach to role
    :return:                        API response
    """
    create_role_response = iam_client.create_role(
        RoleName=role_name,
        AssumeRolePolicyDocument=trust_policy)

    for policy_arn in policy_arns:
        print('\n' + policy_arn + '\n')
        iam_client.attach_role_policy(
            RoleName=role_name,
            PolicyArn=policy_arn
        )

    return create_role_response['Role']


def request_spot_instances(project_id, settings):
    """ Create spot instance request
    :param project_id:              Global project id
    :param settings:                Settings to pass to request
    """
    response = ec2_client.request_spot_instances(**settings)

    spot_request_id = response['SpotInstanceRequests'][0]['SpotInstanceRequestId']

    waiter = ec2_client.get_waiter('spot_instance_request_fulfilled')
    waiter.wait(SpotInstanceRequestIds=[spot_request_id])

    ec2_client.create_tags(Resources=[spot_request_id]
                           , Tags=[{'Key': 'bokchoi-id', 'Value': project_id}])

    response = ec2_client.describe_spot_instance_requests(SpotInstanceRequestIds=[spot_request_id])
    instance_ids = [request['InstanceId'] for request in response['SpotInstanceRequests']]

    ec2_client.create_tags(Resources=instance_ids, Tags=[{'Key': 'bokchoi-id', 'Value': project_id}])


def cancel_spot_request(project_id):
    """ Cancels spot instance request. Request is found by filtering on bokchoi-id tag.
    :param project_id:              Global project id
    """
    print('\nCancelling spot request')
    filters = [{'Name': 'tag:bokchoi-id', 'Values': [str(project_id)]}
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


def terminate_instances(project_id):
    """ Terminates instances. Instances are found by filtering on bokchoi-id tag.
    :param project_id:              Global project id
    """
    print('\nTerminating instances')
    filters = [{'Name': 'tag:bokchoi-id', 'Values': [str(project_id)]}]
    ec2_resource.instances.filter(Filters=filters).terminate()
    print('Instances terminated')


def delete_bucket(project_id):
    """ Delete Bokchoi deploy bucket. Removes all object it contains.
    :param project_id:              Global project id
    """
    print('\nDelete Bucket')

    bucket = s3_resource.Bucket(project_id)

    try:
        bucket.objects.delete()
        bucket.delete()
    except ClientError as e:
        if e.response['Error']['Code'] == 'NoSuchBucket':
            pass
        else:
            raise e


def get_instance_profiles(project_id):
    """ Yields all instance profiles associated with deployment
    :param project_id:              Global project id
    """
    for instance_profile in iam_resource.instance_profiles.all():
        if project_id in instance_profile.instance_profile_name:
            yield instance_profile


def delete_instance_profile(instance_profile):
    """ Deletes instance profile. First removes all roles attached to instance profile.
    :param instance_profile:        Name of instance profile
    """
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


def get_roles(project_id):
    """ Yields all IAM roles associated with deployment
    :param project_id:              Global project id
    :return:                        IAM role
    """
    for role in iam_resource.roles.all():
        if project_id in role.role_name:
            yield role


def delete_role(role):
    """ Deletes IAM role. First detaches all polices from role
    :param role:                    Boto3 Role resource
    """
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


def get_policies(project_id):
    """ Yields all IAM policies associated with deployment
    :param project_id:              Global project id
    :return:                        Boto3 policy resource
    """
    for policy in iam_resource.policies.filter(Scope='Local'):
        if project_id in policy.policy_name:
            yield policy


def delete_policy(policy):
    """ Deletes IAM policy. First detaches all roles from policy.
    :param policy:                  Boto3 policy resource
    """
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


def create_policies(project_id, custom_policy):
    """Creates policies for EMR related tasks"""
    policy_arns = []

    # declare default policy settings
    default_policy_name = project_id + '-default-policy'
    default_policy_document = DEFAULT_POLICY.format(bucket=project_id)
    default_policy_arn = create_policy(default_policy_name, default_policy_document)
    policy_arns.append(default_policy_arn)

    if custom_policy:
        print('Creating custom policy')

        custom_policy_name = project_id + '-custom-policy'
        custom_policy_arn = create_policy(custom_policy_name, custom_policy)
        policy_arns.append(custom_policy_arn)

    return policy_arns


def create_default_role_and_profile(project_id, policy_arns):
    """ Creates default role and instance profile for EC2 deployment.
    :param project_id:              Global project id
    :param policy_arns:             ARN's of policies to attach to default role
    """
    role_name = project_id + '-default-role'
    create_role(role_name, DEFAULT_TRUST_POLICY, *policy_arns)
    create_instance_profile(role_name, role_name)


def create_scheduler(project_id, project, settings):
    """ Creates scheduler. Creates Lambda source bundle, uploads and deploys function
    and creates Cloudwatch Event to trigger scheduler based on specified schedule.
    :param project_id:              Global project id
    :param project:                 Project name
    :param settings:                Project settings
    """
    print('Creating scheduler')

    from bokchoi import scheduler

    file_object = BytesIO()

    requirements = settings.get('Requirements')

    cwd = os.getcwd()

    with zipfile.ZipFile(file_object, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(scheduler.__file__, 'scheduler.py')
        zip_file.write(__file__, 'bokchoi/common.py')
        zip_file.write(ec2.__file__, 'bokchoi/ec2.py')
        zip_file.write(emr.__file__, 'bokchoi/emr.py')
        zip_file.write('\\'.join((cwd, 'bokchoi_settings.json')), 'bokchoi_settings.json')

        zip_file.writestr('__init__.py', '')

        if requirements:
            zip_file.writestr('requirements.txt', '\n'.join(requirements))

    file_object.seek(0)

    bucket_name = project_id
    zip_file_name = 'bokchoi-scheduler.zip'
    upload_zip(bucket_name, file_object, zip_file_name)

    policy_name = project_id + '-scheduler-policy'
    policy_arn = create_policy(policy_name, SCHEDULER_POLICY)

    role_name = project_id + '-scheduler-role'
    response = create_role(role_name, SCHEDULER_TRUST_POLICY, policy_arn)

    # AWS has some specific demands on cron schedule:
    # http://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
    create_function_response = retry(lambda_client.create_function
                                     , FunctionName=project_id + '-scheduler'
                                     , Runtime='python3.6'
                                     , Handler='scheduler.run'
                                     , Role=response['Arn']
                                     , Code={'S3Bucket': bucket_name
                                             , 'S3Key': zip_file_name}
                                     , Timeout=30
                                     , Environment={'Variables': {'project': project}}
                                     , Tags={'bokchoi-id': project_id})
    function_arn = create_function_response['FunctionArn']

    policy_name = project_id + '-event-policy'
    policy_arn = create_policy(policy_name, EVENT_POLICY)

    role_name = project_id + '-event-role'
    response = create_role(role_name, EVENT_TRUST_POLICY, policy_arn)
    role_arn = response['Arn']

    schedule = settings['Schedule']
    create_cloudwatch_rule(project_id, schedule, role_arn, function_arn)


def delete_scheduler(project_id):
    """ Deletes scheduler
    :param project_id:              Global project id
    """
    try:
        lambda_client.delete_function(FunctionName=project_id + '-scheduler')
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceNotFoundException':
            print('Function does not exist')
        else:
            raise e


def create_cloudwatch_rule(project_id, schedule, role_arn, function_arn):
    """ Creates Cloudwatch rule (event) that will trigger lambda function
    :param project_id:              Global project id
    :param schedule:                Event schedule
    :param role_arn:                ARN of role to assign to rule
    :param function_arn:            ARN of Lambda function to trigger
    """
    rule_name = project_id + '-schedule-event'
    retry(events_client.put_rule
          , Name=rule_name
          , ScheduleExpression=schedule
          , State='ENABLED'
          , RoleArn=role_arn)

    events_client.put_targets(Rule=rule_name
                              , Targets=[{'Arn': function_arn, 'Id': '0'}])

    function_name = project_id + '-scheduler'
    lambda_client.add_permission(
        FunctionName=function_name,
        StatementId='0',
        Action='lambda:InvokeFunction',
        Principal='events.amazonaws.com'
    )


def delete_cloudwatch_rule(rule_name):
    """ Delete Cloudwatch rule (event). First removes all targets
    :param rule_name:               Name of rule to remove
    """
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


def create_project_id(project, vendor_specific_id):
    """Creates project id by hashing vendor specific id and project name"""
    return 'bokchoi-' + hashlib.sha1((vendor_specific_id + project).encode()).hexdigest()


def load_settings(project_name):
    """ Loads settings from bokchoi_settings.json. Looking for settings under
    the project name as a key on the lowest level
    :param project_name:            Name of project
    :return:                        Settings
    """
    with open('bokchoi_settings.json', 'r') as settings_file:

        settings = json.load(settings_file)

        try:
            return settings[project_name]
        except KeyError:
            raise KeyError('No config found for {} in bokchoi_settings'.format(project_name))


def zip_package(path, requirements=None):
    """ Creates deployment package by zipping the project directory. Writes requirements to requirements.txt
    if specified in settings
    :param path:                    Path to project directory
    :param requirements:            List of python requirements
    :return:                        Zip file
    """
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
