import argparse


class Context:
    """Context is an application context it holds the state of the running
    application as well as arguments of particular command handler.
    """

    def __init__(self, args):
        self.args = args


class App:
    """App is a shell application."""

    def __init__(self, prog, modules):
        super().__init__()

        self._modules = [m() for m in modules]
        self._parser = argparse.ArgumentParser(prog=prog)

    def setup(self):
        subparsers = self._parser.add_subparsers()

        for m in self._modules:
            m.setup(subparsers)

    def argument(self, args, kwargs):
        self._parser.add_argument(*args, **kwargs)

    def context(self, args):
        return Context(args)

    def start(self):
        args = self._parser.parse_args()
        args.func(self.context(args))
