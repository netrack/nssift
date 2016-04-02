from dnstun.netstats import gauge
from dnstun.netstats.factory import BundlerFactory


def factory():
    """Define a helper to create a new Bundler factory with a
    predefined set of the counters to collect statistics from
    the DNS dumps files."""
    return BundlerFactory(
        # The first counters calculates the Shannon entropy of the
        # DNS requested hostnames, since the most of the DNS tunnels
        # are encoding the arbitrary data into the domain names, which
        # lead to the growth of the entropy value.
        [(gauge.ShannonEntropyGauge, ["meta", "qname"]),

        # This counters calculates the number of the different DNS records
        # types requested from the single IP address, so it could be used
        # as and evidence of the established tunnels.
         (gauge.SetGauge, ["meta", "qtype"])])
