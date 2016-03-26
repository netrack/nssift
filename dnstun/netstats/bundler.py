import logging


LOG = logging.getLogger(__name__)


class Bundler(object):
    """Define a statistics collector mechanism.

    Bundler is accumulating the statistics of the processing
    data by adjusting the list of specified gauges."""

    def __init__(self, gauges):
        """Create a new instance of the bundler.

        gauges: A list of the statistics collectors."""
        self.gauges = gauges

    def process_gauge(self, gauge, params):
        """Update the gauge counter.

        The call will be wrapped into the try-catch to
        prevent the interruptions in updates of other gauges."""
        try:
            gauge.update(params)
        except Exception as error:
            LOG.error(
                "Failed to update the gauge, because of: %(error)s"
                % {"error": error})

    def process(self, params):
        """Update the counters of the gauges."""
        for gauge in self.gauges:
            self.process_gauge(gauge, params)
