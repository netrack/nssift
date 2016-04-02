import abc
import itertools
import operator
import six


@six.add_metaclass(abc.ABCMeta)
class Dissector(object):
    """Define basic dissector interface."""

    @abc.abstractmethod
    def dissect(self, text):
        """Dissect the specified string into the dictionary
        of values.

        This method should be overridden in the derived classes.

        text: A string that should be dissected.
        """

    def stripall(self, lst):
        """Strip the each element of the list of strings.

        lst: An iterable of strings.
        """
        return itertools.imap(operator.methodcaller("strip"), lst)

    def splitnstrip(self, string, symbol=":", maxsplit=1):
        """Split and strip the provided string by the symbol character.

        string:   A string to split.
        symbol:   A splitting symbol.
        maxsplit: A count of maximum splits.
        """
        if symbol not in string:
            return None

        return self.stripall(string.split(symbol, maxsplit))

    @staticmethod
    def partial(*dissectors):
        """Function made of list of dissectors that accepts a list
        of text chunk to be processed

        dissectors: A list of dissector types."""
        dissectors = [klass() for klass in dissectors]

        def _apply_impl(*chunks):
            context = itertools.izip(dissectors, chunks)
            return [d.dissect(t) for d, t in context]

        return _apply_impl
