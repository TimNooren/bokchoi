"""
Schedule tasks using lambda and cloudwatch
"""

import os

PROJECT = os.environ.get('project')


def run(event, context):

    import common
    from ec2 import EC2
    from emr import EMR

    settings = common.load_settings(PROJECT)

    platforms = {'EC2': EC2, 'EMR': EMR}
    platform = platforms.get(settings['Platform'])(PROJECT, settings)

    platform.run()
