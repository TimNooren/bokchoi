
import os
import base64
import click

from . import main


@click.group()
def cli():
    pass


@cli.command('deploy')
@click.argument('project')
def deploy(project):

    settings = main.load_settings(project)

    job_id = main.create_job_id(project)

    requirements = settings.get('Requirements', None)

    bucket = main.create_bucket(settings['Region'], job_id)
    click.secho('Created bucket: ' + bucket, fg='green')

    cwd = os.getcwd()
    zip_file, zip_file_name = main.zip_package(project, cwd, requirements)
    main.upload_zip(bucket, zip_file, zip_file_name)

    role_name = job_id + '-default-role'

    policy_arns = []

    policy_document = main.DEFAULT_POLICY.format(bucket=bucket)
    policy_name = job_id + '-default-policy'
    default_policy_arn = main.create_policy(policy_name, policy_document)
    policy_arns.append(default_policy_arn)

    if settings.get('CustomPolicy'):
        click.echo('Creating custom policy')
        custom_policy = settings['CustomPolicy']

        policy_name = job_id + '-custom-policy'
        custom_policy_arn = main.create_policy(policy_name, custom_policy)
        policy_arns.append(custom_policy_arn)

    main.create_role(role_name, main.TRUST_POLICY, *policy_arns)
    main.create_instance_profile(role_name, role_name)

    if settings.get('Schedule'):

        schedule = settings.get('Schedule')
        main.create_lambda_scheduler(job_id, project, schedule, requirements)


@cli.command('run')
@click.argument('project')
def run(project):
    settings = main.load_settings(project)

    job_id = main.create_job_id(project)

    bucket_name = job_id
    zip_file_name = 'bokchoi-{}.zip'.format(project)

    ec2_settings = settings['EC2']

    app, entry = settings['EntryPoint'].split('.')
    user_data = main.USER_DATA.format(bucket=bucket_name, package=zip_file_name, app=app, entry=entry)
    ec2_settings['LaunchSpecification']['UserData'] = base64.b64encode(user_data.encode('ascii')).decode('ascii')

    ec2_settings['LaunchSpecification']['IamInstanceProfile'] = {'Name': job_id + '-default-role'}

    main.request_spot_instances(job_id, ec2_settings)


@cli.command('undeploy')
@click.argument('project')
def undeploy(project):

    job_id = main.create_job_id(project)

    main.cancel_spot_request(job_id)
    main.terminate_instances(job_id)

    main.delete_bucket(job_id)

    for policy in main.get_policies(job_id):
        main.delete_policy(policy)

    for instance_profile in main.get_instance_profiles(job_id):
        main.delete_instance_profile(instance_profile)

    for role in main.get_roles(job_id):
        main.delete_role(role)

    main.delete_scheduler_lambda(job_id)

    rule_name = job_id + '-schedule-event'
    main.delete_cloudwatch_rule(rule_name)

    click.secho('Undeployed', fg='red')
