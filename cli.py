
import os
import base64
import click
import json

import main


@click.group()
def cli():
    pass


@cli.command('deploy')
@click.argument('project')
def deploy(project):

    settings = main.load_settings()

    job = settings[project]

    job_id = main.create_job_id(project)

    cwd = os.getcwd()
    zip_file_name = 'bokchoi-{}.zip'.format(project)
    if not os.path.isfile('\\'.join((cwd, zip_file_name))):
        zip_file_name = main.zip_package(project)

    if job['EC2']['DryRun']:
        bucket = 'mock_bucket'
    else:
        bucket = main.create_bucket(job['Region'], job_id)
        main.upload_zip(zip_file_name, bucket)

    role_name = job_id + '-default-role'

    policy_arns = []

    policy_document = main.DEFAULT_POLICY.format(bucket=bucket)
    policy_name = job_id + '-default-policy'
    default_policy_arn = main.create_policy(policy_name, policy_document)
    policy_arns.append(default_policy_arn)

    try:
        with open('custom_policy.json', 'r') as custom_policy_file:
            custom_policy = custom_policy_file.read()

        policy_name = job_id + '-custom-policy'
        custom_policy_arn = main.create_policy(policy_name, custom_policy)
        policy_arns.append(custom_policy_arn)
    except IOError:
        pass

    main.create_role(role_name, main.TRUST_POLICY, *policy_arns)
    main.create_instance_profile(role_name, role_name)

    if job.get('Schedule'):

        schedule = job.get('Schedule')
        main.create_lambda_scheduler(job_id, project, schedule)


@cli.command('run')
@click.argument('project')
def run(project):
    settings = main.load_settings()
    job = settings[project]
    job_id = main.create_job_id(project)

    bucket_name = job_id
    zip_file_name = 'bokchoi-{}.zip'.format(project)

    ec2_settings = job['EC2']

    app, entry = job['EntryPoint'].split('.')
    user_data = main.USER_DATA.format(bucket=bucket_name, package=zip_file_name, app=app, entry=entry)
    ec2_settings['LaunchSpecification']['UserData'] = base64.b64encode(user_data.encode('ascii')).decode('ascii')

    ec2_settings['LaunchSpecification']['IamInstanceProfile'] = {'Name': job_id + '-default-role'}

    main.request_spot_instance(job_id, ec2_settings)


@cli.command('undeploy')
@click.argument('project')
def undeploy(project):
    with open('bokchoi_settings.json') as f_setting:
        settings = json.load(f_setting)

    job_id = main.create_job_id(project)

    job = settings[project]
    ec2_settings = job['EC2']
    dry_run = ec2_settings['DryRun']

    main.cancel_spot_request(job_id)
    main.terminate_instances(job_id)
    main.delete_bucket(job_id)

    policy_name = job_id + '-default-policy'
    main.delete_policy(policy_name)

    policy_name = job_id + '-custom-policy'
    main.delete_policy(policy_name)

    policy_name = job_id + '-scheduler-policy'
    main.delete_policy(policy_name)

    instance_profile_name = job_id + '-default-role'
    main.delete_instance_profile(instance_profile_name)

    default_role_name = job_id + '-default-role'
    main.delete_role(default_role_name)

    main.delete_scheduler_lambda(job_id)

    instance_profile_name = job_id + '-scheduler-role'
    main.delete_instance_profile(instance_profile_name)

    scheduler_role_name = job_id + '-scheduler-role'
    main.delete_role(scheduler_role_name)

    rule_name = job_id + '-schedule-event'
    main.delete_cloudwatch_rule(rule_name)

    click.secho('Undeployed', fg='red')
    click.echo(settings.get(project))
