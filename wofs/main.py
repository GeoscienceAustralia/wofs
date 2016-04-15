"""
http://click.pocoo.org/5/commands/
"""
import click

@click.group()
@click.option('--debug/--no-debug', default=False)
def cli(debug):
    click.echo('Debug mode is %s' % ('on' if debug else 'off'))


@cli.command()
def setup():
    click.echo('Initialize WOFS run setup necessary directories, param,')

#----------------------------------------

if __name__== "__main__":
    cli()