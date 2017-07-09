from setuptools import setup

setup(
    name="bokchoi",
    version='0.1',
    packages=['bokchoi'],
    install_requires=[
        'Click',
        'boto3'
    ],
    entry_points='''
        [console_scripts]
        bokchoi=bokchoi.cli:cli
    ''',
)
