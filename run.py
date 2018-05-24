__author__ = "Jeremy Nelson"

import click


@click.command()
@click.option("--server", 
    default="rest", 
    help="Select rest for Falcon API server, async version in the future")
@click.option("--host",
    default="0.0.0.0",
    help="Host IP defaults to 0.0.0.0")
@click.option("--port",
    default=7000,
    help="Port number defaults to 7000") 
def router(server, host, port):
    if server.startswith("async"):
        click.echo("Version not supported")
    elif server.startswith("rest"):
        from server.rest import rest
        from werkzeug.serving import run_simple
        run_simple(
            host,
            port,
            rest,
            use_reloader=True
        )
    else:
        click.echo("Unknown server: {}".format(server))

if __name__ == "__main__":
    router()
