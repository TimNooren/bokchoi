from setuptools import setup

setup(
    name="bokchoi",
    version='0.1',
    py_modules=['cli', 'main', 'scheduler'],
    install_requires=[
        'Click',
        'boto3'
    ],
    entry_points='''
        [console_scripts]
        bokchoi=cli:cli
    ''',
)
