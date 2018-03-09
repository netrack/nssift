import logging

from nssift.pipeline import dissect
from nssift.pipeline import statistics
from nssift.pipeline import clustering
from nssift.netstats import gauge
from nssift.netstats.factory import BundlerFactory


class Cluster(object):
    """Parallel clustering of DNS traffic."""

    def __init__(self, streams):
        """Define a new instance of the cluster context.

        streams: A list of RDD processing streams."""
        super(Cluster, self).__init__()
        self.streams = streams

    def launch(self, sc, params):
        """Perform actual computations.

        sc:     A Spark context instance.
        params: Configuration parameters."""
        rdd = None

        # Launch the streams one by one to process the data.
        for stream in self.streams:
            rdd = stream.launch(sc, rdd, params)

        return rdd


def factory():
    """Define a helper to create a new Bundler factory with a
    predefined set of the counters to collect statistics from
    the DNS dumps files."""
    return BundlerFactory(
        # The first counter calculates the Shannon entropy of the
        # DNS requested hostnames, since the most of the DNS tunnels
        # are encoding the arbitrary data into the domain names, which
        # lead to the growth of the entropy value.
        [(gauge.ShannonEntropyGauge, ["meta", "qname"]),

        # This counter calculates the number of the different DNS records
        # types requested from the single IP address, so it could be used
        # as and evidence of the established tunnels.
         (gauge.SetGauge, ["meta", "qtype"]),

        # This counter calculates the average packet DNS requests from
        # the single host. So based on the host activity we could identify
        # the DNS anomalies.
         (gauge.NumberGauge, ["meta", "query"])])


def streams():
    """Define a helper to create a new list of the Streams to
    process the DNS data."""
    return [
        # The first stream performs the BZip2 archives loading and
        # files processing, so later we could collect statistics.
        dissect.DissectionStream(),

        # One the second step we will perform the statistic collection.
        statistics.StatisticsStream(factory()),

        # Perform the statistics clustering of the aggregated data.
        clustering.ClusteringStream()]
