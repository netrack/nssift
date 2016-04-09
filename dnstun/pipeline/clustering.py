import logging

import numpy
from pyspark.mllib.clustering import KMeans

from dnstun.pipeline import stream

LOG = logging.getLogger(__name__)


class ClusteringStream(stream.Stream):
    """Define the clustering stream."""

    def array(self, value):
        """Translate the result of statistics collection to
        the numpy array."""
        _, bundler = value
        return numpy.array(bundler.normalize())

    def launch(self, sc, rdd, params):
        """Cluster the results of DNS dump processing.

        statistics_rdd: RDD result of the statistics aggregation."""
        # Convert the aggregated statistics to the vectors
        # of the n-dimensional pointers.
        stats_rdd = rdd.map(self.array)

        # Produce the clusters from the aggregated statistics.
        clusters = KMeans.train(stats_rdd, params.clusters)

        print(clusters.centers)
