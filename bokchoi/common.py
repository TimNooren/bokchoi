
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


def upload_zip(bucket_name, zip_file, zip_file_name, fingerprint):
    """ Uploads zip file to S3
    :param bucket_name:             Bucket name
    :param zip_file:                Zipped file
    :param zip_file_name:           Name of zip file in S3
    """
    bucket = s3_resource.Bucket(bucket_name)

    try:
        cur_fingerprint = bucket.Object(zip_file_name).metadata.get('fingerprint')
    except ClientError as e:
        if e.response['Error']['Message'] == 'Not Found':
            print('No package deployed yet. Uploading.')
        else:
            raise e
    else:
        if cur_fingerprint == fingerprint:
            print('Local package matches deployed. Not uploading.')
            return
        else:
            print('Local package does not match deployed. Uploading')

    bucket.put_object(Body=zip_file, Key=zip_file_name, Metadata={'fingerprint': fingerprint})


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
        except ClientError:
            sleep(1)

    raise TimeoutError()


def create_instance_profile(profile_name, role_name=None):
    """ Creates IAM instance profile
    :param profile_name:            Name of profile to be created
    :param role_name:               Name of role to attach to instance profile
    :return:                        API response
    """
    try:
        create_instance_profile_response = iam_client.create_instance_profile(
            InstanceProfileName=profile_name
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print('Instance profile already exists ' + profile_name)
        else:
            raise e
    else:
        if role_name:
            iam_client.add_role_to_instance_profile(
                InstanceProfileName=profile_name,
                RoleName=role_name
            )
        print('Created instance profile: ' + profile_name)
        return create_instance_profile_response['InstanceProfile']


def create_policy(policy_name, document):
    """ Creates IAM policy
    :param policy_name:             Name of policy to create
    :param document:                Policy document associated with policy
    """
    try:
        iam_client.create_policy(PolicyName=policy_name
                                 , PolicyDocument=document)
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print('Policy already exists ' + policy_name)
        else:
            raise e
    else:
        print('Created policy: ' + policy_name)


def create_role(role_name, trust_policy, *policies):
    """ Creates IAM role
    :param role_name:               Name of role to create
    :param trust_policy:            Trust policy to associate with role
    :param policies:                Policies to attach to role
    :return:                        API response
    """
    try:
        iam_client.create_role(RoleName=role_name
                               , AssumeRolePolicyDocument=trust_policy)
    except ClientError as e:
        if e.response['Error']['Code'] == 'EntityAlreadyExists':
            print('Role already exists ' + role_name)
        else:
            raise e
    else:
        for policy in policies:
            if not policy:
                continue
            iam_client.attach_role_policy(
                RoleName=role_name,
                PolicyArn=policy.arn
            )
        print('Created role: ' + role_name)
    return iam_resource.Role(role_name)


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
    """ Cancels spot instance request. Request is found by filtering on project_id tag.
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
    """ Terminates instances. Instances are found by filtering on project_id tag.
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


def get_policies(project_id, pattern=None):
    """ Yields all IAM policies associated with deployment
    :param project_id:              Global project id
    :param pattern:                 Pattern to return specific policies (e.g. default-policy)
    :return:                        Boto3 policy resource
    """
    for policy in iam_resource.policies.filter(Scope='Local'):

        policy_name = policy.policy_name

        if project_id not in policy_name:
            continue

        if pattern and pattern not in policy_name:
            continue

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


def create_lambda_function(project_id, project, role, bucket_name, zip_file_name):

    function_name = project_id + '-scheduler'

    try:
        lambda_client.create_function(FunctionName=function_name
                                      , Runtime='python3.6'
                                      , Handler='scheduler.run'
                                      , Role=role.arn
                                      , Code={'S3Bucket': bucket_name
                                              , 'S3Key': zip_file_name}
                                      , Timeout=30
                                      , Environment={'Variables': {'project': project}}
                                      , Tags={'bokchoi-id': project_id})
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceConflictException':
            print('Scheduler already exists')
        else:
            raise e

    return lambda_client.get_function_configuration(FunctionName=function_name)


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

    with zipfile.ZipFile(file_object, 'w', zipfile.ZIP_DEFLATED) as zip_file:
        zip_file.write(scheduler.__file__, 'scheduler.py')
        zip_file.write(__file__, 'bokchoi/common.py')
        zip_file.write(ec2.__file__, 'bokchoi/ec2.py')
        zip_file.write(emr.__file__, 'bokchoi/emr.py')
        zip_file.write('/'.join((os.getcwd(), 'bokchoi_settings.json')), 'bokchoi_settings.json')

        zip_file.writestr('__init__.py', '')

        if requirements:
            zip_file.writestr('requirements.txt', '\n'.join(requirements))

    file_object.seek(0)

    bucket_name = project_id
    zip_file_name = 'bokchoi-scheduler.zip'
    upload_zip(bucket_name, file_object, zip_file_name, fingerprint='n/a')

    scheduler_policy_name = project_id + '-scheduler-policy'
    create_policy(scheduler_policy_name, SCHEDULER_POLICY)
    scheduler_policy = next(get_policies(project_id, pattern=scheduler_policy_name))

    scheduler_role_name = project_id + '-scheduler-role'
    role = create_role(scheduler_role_name, SCHEDULER_TRUST_POLICY, scheduler_policy)

    # AWS has some specific demands on cron schedule:
    # http://docs.aws.amazon.com/lambda/latest/dg/tutorial-scheduled-events-schedule-expressions.html
    lambda_function = retry(create_lambda_function
                            , project_id=project_id
                            , project=project
                            , role=role
                            , bucket_name=bucket_name
                            , zip_file_name=zip_file_name)

    event_policy_name = project_id + '-event-policy'
    create_policy(event_policy_name, EVENT_POLICY)
    event_policy = next(get_policies(project_id, pattern=event_policy_name))

    event_role_name = project_id + '-event-role'
    event_role = create_role(event_role_name, EVENT_TRUST_POLICY, event_policy)

    schedule = settings['Schedule']
    create_cloudwatch_rule(project_id, schedule, event_role.arn, lambda_function['FunctionArn'])


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
    print('Scheduling job using ' + schedule)
    rule_name = project_id + '-schedule-event'
    retry(events_client.put_rule
          , Name=rule_name
          , ScheduleExpression=schedule
          , State='ENABLED'
          , RoleArn=role_arn)

    events_client.put_targets(Rule=rule_name
                              , Targets=[{'Arn': function_arn, 'Id': '0'}])

    function_name = project_id + '-scheduler'
    try:
        lambda_client.add_permission(
            FunctionName=function_name,
            StatementId='0',
            Action='lambda:InvokeFunction',
            Principal='events.amazonaws.com'
        )
    except ClientError as e:
        if e.response['Error']['Code'] == 'ResourceConflictException':
            print('Scheduler rule already exists')
        else:
            raise e


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

        fingerprint = '|'.join([str(elem.CRC) for elem in zip_file.infolist()])

    file_object.seek(0)

    return file_object, fingerprint
