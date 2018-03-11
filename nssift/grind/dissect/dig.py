import itertools
import logging
import re

from nssift.grind.dissect.dissector import Dissector


LOG = logging.getLogger(__name__)


class Dig(Dissector):
    """Define dig-utility response dissector."""

    # The example of the text data, that should be dissected.
    #
    # ;; ->>HEADER<<- opcode: QUERY, rcode: NOERROR, id: 4291
    # ;; flags: qr; QUERY: 1, ANSWER: 0, AUTHORITY: 4, ADDITIONAL: 8
    #
    # ;; QUESTION SECTION:
    # ;ya.gds.tmall.com. IN A
    #
    # ;; ANSWER SECTION:
    #
    # ;; AUTHORITY SECTION:
    # gds.tmall.com. 86400 IN NS gdsns1.taobao.com.
    # gds.tmall.com. 86400 IN NS gdsns2.taobao.com.
    #
    # ;; ADDITIONAL SECTION:
    # gdsns1.tmall.com. 3600 IN A 140.205.122.66
    # gdsns1.tmall.com. 3600 IN A 198.11.138.254

    # A regular expression for a request/response header dissection.
    header_dissector = re.compile("->>HEADER<<- ([\w\ ,:]+)")

    # A regular expression for a request/response flag dissection.
    flags_dissector = re.compile("flags:([\w\ ]*); ([\w\ ,:]+)")

    def fetch_header(self, text):
        """Dissect the header of the response."""
        match = self.header_dissector.search(text)
        if not match:
            return None

        params = match.group(1)

        # Fetch the pairs of key-value items from the header.
        pairs = map(self.splitnstrip, params.split(","))

        # Return a dictionary with parsed header.
        return dict(filter(None, pairs))

    def fetch_flags(self, text):
        """Dissect the flags of the response."""
        match = self.flags_dissector.search(text)
        if not match:
            return None

        flags = match.group(1)
        params = match.group(2)

        # Fetch the pairs of key-value items from the flags.
        pairs = map(self.splitnstrip, params.split(","))

        # Return a dictionary with parsed flags.
        attrs = dict(filter(None, pairs))
        if not attrs:
            return None

        attrs.update({"flags": flags.strip()})
        return attrs

    def fetch_section(self, text):
        """Dissect the sections of the response"""
        key, value = self.splitnstrip(text)
        # Modify the section name. Example: ANSWER SECTION -> answer_section.
        key = key.replace(" ", "_").lower()
        return key, value

    def dissect(self, text):
        """Dissect the specified text and produce the dictionary
        of parsed values.

        text: A string with dumped dig requests/responses."""
        sections = list(self.stripall(text.strip().split(";;")))

        # Strip the leading newline row.
        sections = sections[1:]

        # The response have a regular structure, therefore if something
        # is missing, we should skip them.
        #
        # So at this point we are expecting request or response header,
        # the set of flags, and four sections: (question section, answer
        # section, authority section, additional section).
        #
        # Note, that additional section could be omitted, since expect
        # that at leas five will be presented.
        if len(sections) < 5:
            LOG.debug("Failed to dissect the text file due to "
                      "wrong sections count: '%(text)s'" % {"text": text})
            return None

        # Set the next steps of the text parsing, since it looks
        # like the header does not have correct format.
        header = self.fetch_header(sections[0])
        if not header:
            LOG.debug("Failed to dissect the header section: '%(header)s'"
                      % {"header": sections[0]})
            return None

        # The same with flags, probably the provided text is
        # not valid.
        flags = self.fetch_flags(sections[1])
        if not flags:
            LOG.debug("Failed to dissect the flags section: '%(flags)s'"
                      % {"flags": sections[1]})
            return None

        # All other items will be treated as regular sections.
        sections = dict(map(self.fetch_section, sections[2:]))
        sections.update({"header": header, "flags": flags})

        return sections
