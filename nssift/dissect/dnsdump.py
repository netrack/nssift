import re

from nssift.dissect.dig import Dig
from nssift.dissect.proto import Proto

from nssift.dissect.dissector import Dissector


class DnsDump(Dissector):
    """Define a helper to parse the chunk of the DNS dump
    that will be ready to be consumed by statistics bundler."""

    # Define a dissector for response and query parameters.
    octets_dissector = re.compile(r"\[(\d+) octets\]")

    def populate(self, meta_params, packet_params):
        """Package with the type and identifier if the specified
        dictionaries contain valid data."""
        packet_id = packet_params.get("header", {}).get("id")
        packet_type = None

        if "query" in meta_params:
            packet_type = "REQUEST"
        elif "response" in meta_params:
            packet_type = "RESPONSE"

        # Not a valid data, just throw it away.
        if packet_type is None or packet_id is None:
            return None

        # Define a DNS exchange message payload and data.
        payload = {"type": packet_type,
                   "meta": meta_params,
                   "packet": packet_params}

        # Return the transaction identifiers as like as a list of
        # payloads. We will return a list, so it will be easier
        # later to aggregate the requests and responses by the same
        # identifier value.
        return {"id": packet_id, "transaction": [payload]}

    def fetch_octets(self, params, attribute):
        """Fetch the value of octets of the specified attribute."""
        value = params.get(attribute)

        # If the value is not provided is the specified
        # dictionary, the we should skip the processing
        if not value:
            return params

        # Try to dissect octets.
        match = self.octets_dissector.search(value)
        if not match:
            return params

        octets = match.group(1)
        # We could safely convert the group to the integer
        # value, because the regular expression matched that
        # it is containing only numbers.
        params[attribute] = int(octets)
        return params

    def dissect(self, text):
        """Parse the specified text either as a DNS request, or as
        a DNS response."""
        # Split the specified text into two pieces.
        splitindex = text.find(";;")

        # If count of pieces is not equal to two, that means
        # that the specified text chunk is probably broken, and
        # the further processing is useless.
        if splitindex == -1:
            return None

        dissect_functor = self.partial(Proto, Dig)
        meta, packet = dissect_functor(
            text[:splitindex], text[splitindex:])

        # Both dissections should be valid.
        if not (meta and packet):
            return None

        # Pre-process the "query" and "response" parameters
        # to retrieve the count of octets send or received.
        meta = self.fetch_octets(meta, "query")
        meta = self.fetch_octets(meta, "response")

        # Generate a new dissected data package.
        return self.populate(meta, packet)
