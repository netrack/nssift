import logging
from nssift.pipeline import stream

LOG = logging.getLogger(__name__)


class StatisticsStream(stream.Stream):
    """Define a stream to process the statistics collection.
    Calculate the statistics for each IP address.
    """

    def __init__(self, factory):
        """Initialize a new instance of the statistics
        collection stream.

        factory: A bundler factory instance."""
        super(StatisticsStream, self).__init__()
        self.factory = factory

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

    def launch(self, sc, rdd, params):
        """Second stage of the DNS dumps processing is to perform
        the actual data collection.

        rdd: RDD result of the files dissection."""
        # Span each element of the RDD into the pair of request
        # or response identifier and the its respective content.
        exchanges_rdd = rdd.map(self.span(["id"]))
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
