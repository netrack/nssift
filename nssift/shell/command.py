import argparse
import abc
import collections
import six


CommandAttributes = collections.namedtuple(
    "CommandAttributes", ("name", "aliases", "arguments", "help")
)


class Command(metaclass=abc.ABCMeta):
    """Command is an abstract command."""

    def __init__(self):
        super().__init__()
        attrs = {}

        # Copy class attributes into the meta information of the command.
        for attr in CommandAttributes._fields:
            attrs[attr] = getattr(self, attr, None)

        self._command_chain = []
        self.subcommands = {}
        self.__meta__ = CommandAttributes(**attrs)

    def setup(self, subparsers):
        """Hierarhical setup of the command."""

        self.subparser = subparsers.add_parser(
            name=self.__meta__.name,
            help=self.__meta__.help,
            aliases=(self.__meta__.aliases or []))

        for args, kwargs in self.__meta__.arguments or []:
            self.subparser.add_argument(*args, **kwargs)

        class_dict = self.__class__.__dict__
        filter_predicate = lambda o: isinstance(o, type)

        # Search for attributes that are instances of the type, so they
        # are treated as sub-commands of this command.
        subcommands = filter(filter_predicate, class_dict.values())
        subcommands = list(subcommands)
        if not subcommands:
            self.subparser.set_defaults(func=self.handle)
            return

        subparsers = self.subparser.add_subparsers()

        for command_class in subcommands:
            command = command_class()
            command.setup(subparsers)
            self.subcommands[command_class.__name__] = command

    @abc.abstractmethod
    def handle(self, context):
        """Handle the execution of the command."""
        pass
