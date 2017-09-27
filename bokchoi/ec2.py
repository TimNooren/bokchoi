#!/usr/bin/env python3.6
"""
Class which can be used to deploy and run EC2 spot instances
"""
import click


class EC2(object):
    """Create EC2 object which can be used to schedule jobs"""
    def __init__(self, project, settings):
        pass

    def deploy(self):
        """Zip package and deploy to S3"""
        pass

    def undeploy(self):
        """Deletes all policies, users, and instances permanently"""
        pass

    def run(self):
        """Create EC2 machine with given AMI and instance settings"""
        click.echo("Running EC2 instance")
