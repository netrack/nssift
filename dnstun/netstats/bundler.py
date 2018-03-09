import itertools
import operator
import logging


LOG = logging.getLogger(__name__)


class Bundler(object):
    """Define a statistics collector mechanism.

    Bundler is accumulating the statistics of the processing
    data by adjusting the list of specified gauges."""

    def __init__(self, gauges):
        """Create a new instance of the bundler.

        gauges: A list of the statistics collectors."""
        super(Bundler, self).__init__()
        self.gauges = gauges

    def safexec(self, func, params):
        """Execute the specified function with paramers.

        The call will be wrapped into the try-catch to
        prevent the interruptions in updates of other gauges."""
        try:
            func(params)
        except Exception as error:
            LOG.error("Failed to update the gauge, because "
                      "of: %(error)s" % {"error": error})

    def update(self, params):
        """Update the counters with the specified piece
        of the information.

        params: A statistic chunk."""
        for gauge in self.gauges:
            self.safexec(gauge.update, params)

    def updateall(self, params):
        """Updatee the countes with the specified list
        of the information.

        params: A list of statistic chunks."""
        for gauge in self.gauges:
            self.safexec(gauge.updateall, params)

    def join(self, other):
        """Join the respective results of the other bundler
        counters.

        other: A Bundler instance to join."""
        # In fact we simply will join the counters of this bundler with the
        # counters of the specified bundler.
        pairs = zip(self.gauges, other.gauges)

        # Wrap the call into the list conversion, since the imap method returns
        # a generator.
        list(map(lambda ab: ab[0].join(ab[1]), pairs))

        # It is important to return the referece to ourselves,
        # as it will be used as an accumulator in the reduce call.
        return self

    def normalize(self):
        """Array of the normalized counter values."""
        normalized = map(
            operator.methodcaller("normalize"), self.gauges)

        # Convert the result to the list of normalized
        # float values.
        return list(map(float, normalized))
