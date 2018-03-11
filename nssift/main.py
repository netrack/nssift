import nssift
import nssift.shell
import nssift.shell.commands.grind


def main():
    app = nssift.shell.App(prog="nssift", modules=[
        nssift.shell.commands.grind.Grind,
    ])

    app.setup()
    app.argument(
        ["-v", "--version"],
        dict(help="print version and exit",
             action="version",
             version=nssift.__version__))

    app.start()
