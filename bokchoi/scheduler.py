"""
Schedule tasks using lambda and cloudwatch
"""
import base64
import os


PROJECT = os.environ.get('project')


def run(event, context):

    from . import helper

    settings = helper.load_settings(PROJECT)

    job_id = helper.create_job_id(PROJECT)

    bucket_name = job_id
    zip_file_name = 'bokchoi-{}.zip'.format(PROJECT)

    ec2_settings = settings['EC2']

    app, entry = settings['EntryPoint'].split('.')
    user_data = helper.USER_DATA.format(bucket=bucket_name, package=zip_file_name, app=app, entry=entry)
    ec2_settings['LaunchSpecification']['UserData'] = base64.b64encode(user_data.encode('ascii')).decode('ascii')

    ec2_settings['LaunchSpecification']['IamInstanceProfile'] = {'Name': job_id + '-default-role'}

    helper.request_spot_instances(job_id, ec2_settings)
