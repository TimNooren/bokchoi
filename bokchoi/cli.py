#!/usr/bin/env python3.6
"""
Main cli program which allows execution of commands
"""

import click

from bokchoi import Bokchoi


@click.group()
def cli():
    pass


def get_bokchoi(func):
    """ Decorator that instantiates the Bokchoi backend using the project_name
    and (optional) config parameters and passes it as the first argument.
    :param func:                Function
    :return:                    Decorated function
    """
    @click.option('--name', '-n', required=False)
    @click.option('--path', '-p', default='', help="Application path")
    def wrapped(name, path, *args, **kwargs):
        func(Bokchoi(name, path), *args, **kwargs)
    return wrapped


@cli.command('init', help='Initialise new project')
@click.option('--platform', '-f', default='EC2')
@get_bokchoi
def init(bokchoi, platform):
    res = bokchoi.init(platform)
    click.secho(res.message, fg=res.color)


@cli.command('deploy', help='Deploy your project')
@get_bokchoi
def deploy(bokchoi):
    bokchoi.deploy()
    click.secho('Deployed', fg='green')


@cli.command('undeploy', help='Remove your project deployment')
@click.option('--dryrun', is_flag=True, default=False, help="Only prints actions")
@get_bokchoi
def undeploy(bokchoi, dryrun):
    bokchoi.undeploy(dryrun)
    click.secho('Undeployed', fg='green')


@cli.command('run', help='Run your application')
@get_bokchoi
def run(bokchoi):
    bokchoi.run()
    click.secho('Running application', fg='green')


@cli.command('stop', help='Stop any running applications')
@click.option('--dryrun', is_flag=True, default=False, help="Print in stead of terminate")
@get_bokchoi
def stop(bokchoi, dryrun):
    bokchoi.stop(dryrun)


@cli.command('connect', help='Connect to your running application')
@click.option('--local-port', help='Local port to bind to')
@click.option('--remote-port', help='Remote port to bind to')
@get_bokchoi
def connect(bokchoi, local_port, remote_port):
    bokchoi.connect(local_port, remote_port)


@cli.command('status', help='Connect to your running application')
@get_bokchoi
def status(bokchoi):
    bokchoi.status()
