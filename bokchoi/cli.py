#!/usr/bin/env python3.6
"""
Main cli program which allows execution of commands
"""
import sys
import click

from . import helper
from .ec2 import EC2
from .emr import EMR


@click.group()
@click.argument('project')
@click.pass_context
def cli(ctx, project):
    """Starting point for cli commands"""
    settings = helper.load_settings(project)

    # instantiate object based on given platform parameter in settings file
    ctx.obj = {}
    types = {'EC2': EC2(project, settings), 'EMR': EMR(project, settings)}
    ctx.obj = types.get(settings.get('Platform'))

    if not types:
        click.echo("Choose a supported instance type option..")
        sys.exit(1)

@cli.command('run')
@click.pass_obj
def run(ctx):
    """Default run command"""
    ctx.run()
    click.secho('Running application', fg='green')

@cli.command('deploy')
@click.pass_obj
def deploy(ctx):
    """Default deploy command"""
    ctx.deploy()
    click.secho('Deployed', fg='green')

@cli.command('undeploy')
@click.pass_obj
def undeploy(ctx):
    """Default undeploy command"""
    ctx.undeploy()
    click.secho('Undeployed', fg='green')
