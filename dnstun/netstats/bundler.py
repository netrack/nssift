import itertools
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

    def updateone(self, gauge, params):
        """Update the gauge counter.

        The call will be wrapped into the try-catch to
        prevent the interruptions in updates of other gauges."""
        try:
            gauge.update(params)
        except Exception as error:
            LOG.error("Failed to update the gauge, because "
                      "of: %(error)s" % {"error": error})

    def update(self, params):
        """Update the counters of the gauges."""
        for gauge in self.gauges:
            self.updateone(gauge, params)

    def join(self, other):
        """Join the respective results of the other bundler
        counters.

        other: A Bundler instance to join."""
        # In fact we simply will join the counters of this
        # bundler with the counters of the specified bundler.
        pairs = itertools.izip(self.gauges, other.gauges)

        # Wrap the call into the list conversion, since the
        # imap method returns a generator.
        list(itertools.imap(lambda (a, b): a.join(b), pairs))
