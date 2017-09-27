"""
Information about the class
"""
import os
import sys
import base64
from enum import Enum
import click

from . import helper
from .ec2 import EC2
from .emr import EMR


@click.group()
@click.argument('project')
@click.pass_context
def cli(ctx, project):
    settings = helper.load_settings(project)
    job_id = helper.create_job_id(project)

    types = {'EC2': EC2(), 'EMR': EMR(job_id)}
    ctx.obj = types.get(settings.get('Platform'))

    if not types:
        click.echo("Choose a supported instance type option..")
        sys.exit(1)

@cli.command('run')
@click.pass_obj
def run(ctx):
    ctx.run()

@cli.command('deploy')
@click.pass_obj
def deploy(ctx):
    click.echo('Dropped the database')
    ctx.deploy()


"""
@cli.command('deploy')
@click.argument('project')
def deploy(project):

    requirements = settings.get('Requirements', None)

    bucket = main.create_bucket(settings['Region'], job_id)
    click.secho('Created bucket: ' + bucket, fg='green')

    zip_file_name = 'bokchoi-' + project + '.zip'
    cwd = os.getcwd()
    zip_file = main.zip_package(cwd, requirements)
    main.upload_zip(bucket, zip_file, zip_file_name)

    role_name = job_id + '-default-role'

    policy_arns = []

    default_policy_name = job_id + '-default-policy'
    default_policy_document = main.DEFAULT_POLICY.format(bucket=bucket)
    default_policy_arn = main.create_policy(default_policy_name, default_policy_document)
    policy_arns.append(default_policy_arn)

    if settings.get('CustomPolicy'):
        click.echo('Creating custom policy')

        custom_policy_name = job_id + '-custom-policy'
        custom_policy_document = settings['CustomPolicy']
        custom_policy_arn = main.create_policy(custom_policy_name, custom_policy_document)
        policy_arns.append(custom_policy_arn)

    main.create_role(role_name, main.TRUST_POLICY, *policy_arns)
    main.create_instance_profile(role_name, role_name)

    if settings.get('Schedule'):
        schedule = settings['Schedule']
        click.echo('Scheduling job using ' + schedule)
        main.create_scheduler(job_id, project, schedule, requirements)


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
 
    main.delete_scheduler(job_id)

    rule_name = job_id + '-schedule-event'
    main.delete_cloudwatch_rule(rule_name)

    click.secho('Undeployed', fg='red')
"""