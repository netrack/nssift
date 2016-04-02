import logging
import operator

from dnstun.fileutil.bzloader import BzLoader
from dnstun.dissect.dnsdump import DnsDump

LOG = logging.getLogger(__name__)


class Cluster(object):
    """Parallel clustering of DNS traffic."""

    # Define a splitting string that will be used to divide
    # the DNS dump files into the chunks.
    splitstring = "---"

    def __init__(self, factory):
        """Define a new instance of the cluster context.

        factory: A bundler factory instance."""
        super(Cluster, self).__init__()
        self.factory = factory

    def uncompress(self, filename):
        """Generator of the compressed DNS request/response
        chunks.

        filename: A compressed DNS dump."""
        return BzLoader(filename).load(self.splitstring)

    def dissect(self, text):
        """Dissect the chunks of the DNS dumps, so we could
        calculate the actual statistics.

        text: A DNS dump chunk."""
        return DnsDump().dissect(text)

    def nonefilter(self, value):
        """True if the value is not equal to None and False
        otherwise."""
        return value is not None

    def launch_file_dissection(self, sc, params):
        """First stage of the processing bzip2 archives with
        DNS dump is to uncompress the data and perform the
        text dividing into the chunks."""
        # Parallelize the archives processing.
        filenames_rdd = sc.parallelize(
            BzLoader.isearch(params.source_path_name))

        LOG.info("Searching for the DNS dumps archives in the "
                 "folder: '%(source_path_name)s'." %
                 {"source_path_name": params.source_path_name})

        # For each the bzip archive, load the content and split it
        # into the chunks that could be dissected later.
        filechunks_rdd = filenames_rdd.flatMap(self.uncompress)
        LOG.info("Uncompressing the DNS dumps files content.")

        # Try to parse every piece of the DNS dump and put
        # the collected information into the easy-to-use
        # dictionary.
        dissections_rdd = filechunks_rdd.map(self.dissect)
        LOG.info("Dissecting the DNS requests and responses.")

        # The dissection could fail on the corrupted data, in this
        # case it will be resulted in the None values, so we have
        # to filter them out.
        dissections_rdd = dissections_rdd.filter(self.nonefilter)
        LOG.info("Filtering unsuccessful dissections.")

        # Return the result data set parsed and filtered. Now data
        # should be ready for statistics collection.
        return dissections_rdd

    def _getattr(self, keys, value):
        """Value of the deeply nested key."""
        for key in keys:
            value = value.get(key)
        return value

    def span(self, keys):
        """A spanning function, that produce a tuple of key,
        extracted by the specified list of keys, and the value.

        keys: A list of nested keys."""
        return lambda value: (self._getattr(keys, value), value)

    def join_transaction(self, a, b):
        """Group the packets payloads of the same transaction.

        a, b: A request or response DNS packet."""
        # Simply extend the list of packets.
        a["transaction"].extend(b["transaction"])
        return a

    def span_host(self, keypair):
        """Span the transaction into the tuple of source IP
        address and the bundle of counters."""
        _, value = keypair
        # At first, we are going to find the IP address
        # to use it as a key.
        transaction = value.get("transaction", [])

        for payload in transaction:
            query_ip = self._getattr(["meta", "query_ip"], payload)

            # Looks like this payload does not have the IP address
            # so lets look into the another one.
            if not query_ip:
                continue

            # Define a new bundler using the factory. Update the
            # bundler counters with the transaction data.
            bundler = self.factory.build()
            bundler.updateall(transaction)

            # Return a pair of query IP address and the counters.
            return query_ip, bundler

    def join_host(self, a, b):
        """Aggregate the statistics for each host.

        a, b: A host statistic counters."""
        # Return the join of the statistics.
        return a.join(b)

    def launch_statistics_collection(self, dissections_rdd):
        """Second stage of the DNS dumps processing is to perform
        the actual data collection.

        dissections_rdd: RDD result of the files dissection."""
        # Span each element of the RDD into the pair of request
        # or response identifier and the its respective content.
        exchanges_rdd = dissections_rdd.map(self.span(["id"]))
        LOG.info("Spanning the dissections into id-based groups.")

        # Group the request/responses into the single transaction.
        transactions_rdd = exchanges_rdd.reduceByKey(self.join_transaction)
        LOG.info("Grouping the transactions by the identifier.")

        # At this step we will generate the pairs with an IP address
        # as a key and a Bundler instance as a value.
        #
        # It is required, since later we will join the bundler results
        # by the IP address.
        hosts_rdd = transactions_rdd.map(self.span_host)
        LOG.info("Spanning the transactions into IP-based groups.")

        # The above mapping could return None if the IP address
        # for some reason was not defined in the transaction
        # metadata. Therefore we should filter the results.
        hosts_rdd = hosts_rdd.filter(self.nonefilter)
        LOG.info("Filtering the invalid transactions.")

        # Now we are going to aggregate the statistics for each
        # IP address participated in the DNS activity.
        statistics_rdd = hosts_rdd.reduceByKey(self.join_host)
        LOG.info("Gathering statistics for each IP address.")

        # Return the result statistics for further processing.
        return statistics_rdd

    def launch(self, sc, params):
        """Perform actual computations.

        sc:     A Spark context instance.
        params: Configuration paramers."""
        # Search for the DNS dump archives and parse theirs content.
        dissections_rdd = self.launch_file_dissection(sc, params)

        # Calculate the statistics for each IP address.
        statistics_rdd = self.launch_statistics_collection(dissections_rdd)

        print list(statistics_rdd.collect())
