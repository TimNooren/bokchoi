
import os
from setuptools import setup


try:
    from pypandoc import convert
    README = convert('README.md', 'rst')
except ImportError:
    README = open(os.path.join(os.path.dirname(__file__), 'README.md'), 'r', encoding="utf-8").read()

setup(
    name="bokchoi",
    version="0.4.4",
    packages=['bokchoi', 'bokchoi.aws', 'bokchoi.gcp'],
    package_dir={'bokchoi.aws': 'bokchoi/aws',
                 'bokchoi.gcp': 'bokchoi/gcp'},
    package_data={'bokchoi.aws': ['ec2-startup-script.sh'],
                  'bokchoi.gcp': ['gcp-startup-script.sh']},
    install_requires=[
        'Click',
        'boto3',
        'botocore',
        'paramiko',
        'google-api-python-client',
        'google-auth-httplib2',
        'google-cloud-storage'
    ],
    url='https://github.com/TimNooren/bokchoi',
    author='Tim Nooren',
    author_email='timnooren@gmail.com',
    long_description=README,
    license='MIT',
    entry_points={
        'console_scripts': [
            'bokchoi=bokchoi.cli:cli'
        ]
    },
    classifiers=[
        'Development Status :: 3 - Alpha',
        'Environment :: Console',
        'License :: OSI Approved :: MIT License',
        'Operating System :: OS Independent',
        'Natural Language :: English',
        'Programming Language :: Python',
        'Programming Language :: Python :: 3.5',
    ],
)
