
import hashlib
from time import sleep
import urllib
from io import BytesIO
import os
import zipfile

from bokchoi.aws import cloudwatch_logger


def retry(func, exc, **kwargs):
    """ Retries boto3 function call in case a ClientError occurs
    :param func:                    Function to call
    :param exc:                     Exception to catch
    :param kwargs:                  Parameters to pass to function
    :return:                        Function response
    """
    for _ in range(60):
        try:
            response = func(**kwargs)
            return response
        except exc:
            sleep(1)

    raise TimeoutError()


def create_project_id(project_name, vendor_specific_id):
    """Creates project id by hashing vendor specific id and project name"""
    unique_id = hashlib.sha1((vendor_specific_id + project_name).encode()).hexdigest()
    return '-'.join(('bokchoi', project_name, unique_id[:12]))


def get_my_ip():
    with urllib.request.urlopen('https://api.ipify.org/') as response:
        return response.read().decode('utf8')


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

        zip_file.write(cloudwatch_logger.__file__, 'cloudwatch_logger.py')

        zip_file.writestr('requirements.txt', '\n'.join(requirements or ''))

        fingerprint = '|'.join([str(elem.CRC) for elem in zip_file.infolist()])

    file_object.seek(0)

    return file_object, fingerprint
