
import base64
import os


PROJECT = os.environ.get('project')


def run(event, context):

    import main

    settings = main.load_settings()
    job = settings[PROJECT]
    job_id = main.create_job_id(PROJECT)

    bucket_name = job_id
    zip_file_name = 'bokchoi-{}.zip'.format(PROJECT)

    ec2_settings = job['EC2']

    app, entry = job['EntryPoint'].split('.')
    user_data = main.USER_DATA.format(bucket=bucket_name, package=zip_file_name, app=app, entry=entry)
    ec2_settings['LaunchSpecification']['UserData'] = base64.b64encode(user_data.encode('ascii')).decode('ascii')

    ec2_settings['LaunchSpecification']['IamInstanceProfile'] = {'Name': job_id + '-default-role'}

    main.request_spot_instance(job_id, ec2_settings)
