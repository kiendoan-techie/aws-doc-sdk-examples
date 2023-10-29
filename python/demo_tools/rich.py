def setup():
    import logging
    from rich import print as nprint
    from rich.panel import Panel
    from rich.text import Text
    from rich.logging import RichHandler
    from rich.console import Console

    console = Console()
    print = console.rule

    FORMAT = "%(message)s"
    logging.basicConfig(
        level=logging.INFO, datefmt="[%X]", handlers=[RichHandler()]
    )

    def header(message):
        nprint(Panel(Text(message, style="bold magenta", justify="center")))

    return nprint, Panel, Text, RichHandler, console, print, header

