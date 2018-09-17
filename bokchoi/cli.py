#!/usr/bin/env python3.6
"""
Main cli program which allows execution of commands
"""

import click

from bokchoi import Bokchoi


@click.group()
def cli():
    pass


@cli.command('init', help='Initialise new project')
@click.argument('name')
@click.option('--directory', '-d', default='.', help="Application directory")
@click.option('--platform', '-f', default='EC2')
def init(name, directory, platform):
    response = Bokchoi(directory).init(name, platform)
    click.secho(response, fg='green')


@cli.command('deploy', help='Deploy your project')
@click.option('--directory', '-d', default='.', help="Application directory")
def deploy(directory):
    response = Bokchoi(directory).deploy()
    click.secho(response, fg='green')


@cli.command('undeploy', help='Remove your project deployment')
@click.option('--directory', '-d', default='.', help="Application directory")
@click.option('--dryrun', is_flag=True, default=False, help="Only prints actions")
def undeploy(directory, dryrun):
    response = Bokchoi(directory).undeploy(dryrun)
    click.secho(response, fg='green')


@cli.command('run', help='Run your application')
@click.option('--directory', '-d', default='.', help="Application directory")
def run(directory):
    response = Bokchoi(directory).run()
    click.secho(response, fg='green')


@cli.command('stop', help='Stop any running applications')
@click.option('--directory', '-d', default='.', help="Application directory")
@click.option('--dryrun', is_flag=True, default=False, help="Print in stead of terminate")
def stop(directory, dryrun):
    response = Bokchoi(directory).stop(dryrun)
    click.secho(response, fg='green')


@cli.command('connect', help='Connect to your running application')
@click.option('--directory', '-d', default='.', help="Application directory")
@click.option('--local-port', help='Local port to bind to')
@click.option('--remote-port', help='Remote port to bind to')
def connect(directory, local_port, remote_port):
    Bokchoi(directory).connect(local_port, remote_port)


@cli.command('status', help='Status of deployed project')
@click.option('--directory', '-d', default='.', help="Application directory")
def status(directory):
    Bokchoi(directory).status()


@cli.command('logs', help='View logs of current or latest run')
@click.option('--directory', '-d', default='.', help="Application directory")
def logs(directory):
    Bokchoi(directory).logs()
