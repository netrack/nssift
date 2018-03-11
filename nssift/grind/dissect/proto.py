import itertools
import operator

from nssift.grind.dissect.dissector import Dissector


class Proto(Dissector):
    """Define DNS request/response dissector."""

    def dissect(self, text):
        """Dissect the text and produce the dictionary
        of parsed values.

        text: A string with dumped DNS requests/responses."""
        # Split the text into the lines, then parse each line
        # separately.
        pairs = map(self.splitnstrip, text.split("\n"))

        # Return a dictionary with non-empty dissected elements.
        return dict(filter(None, pairs))
