"""
http://click.pocoo.org/5/commands/
"""
import click
import sys
from workflow.wofs_setup import WofsSetup

@click.group()
@click.option('--debug/--no-debug', default=False)
def cli(debug):
    click.echo('Debug mode is %s' % ('on' if debug else 'off'))

###################################
@cli.command()
@click.option('--infile', help='yml file')
@click.option('--template_conf', default=None, help='Template client.cfg')
def setup(infile, template_conf):
    click.echo('Initialize WOFS run with a working dir directory and conf param,')
    if infile is None:
        print "The essential input is missing"
        sys.exit(1)

    wofs=WofsSetup(infile )

    wofs.main()

###################################
@cli.command()
def query():
    click.echo('Query datacube for NBAR QPA, etc inputs for Wofs')

###################################
@cli.command()
def mkext():
    click.echo('Create water extents maps from the NBAR QPA, etc inputs.')


###################################
@cli.command()
def sum():
    click.echo('Wofs summary 3-in-1 up-and-running in NCI PBS system')


#######################################################################
# python main.py setup --infile ./workflow/wofs_input.yml
#######################################################################
if __name__== "__main__":

    cli()
