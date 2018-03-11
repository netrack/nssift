import abc
import six

from nssift.grind.netstats.bundler import Bundler


class BaseFactory(metaclass=abc.ABCMeta):
    """A bundler factory used to create a set of the
    pre-defined counters."""

    @abc.abstractmethod
    def build(self):
        """Create a new instance of the bundler.

        This method should be overridden in the
        derived classes."""


class BundlerFactory(BaseFactory):
    """Define a regular bundler factory."""

    def __init__(self, gauges):
        """Create a new instance of the bundler factory
        with the specified set of counters.

        gauges: A list of the tuples of gauge type and
                type constructor arguments.

                Example: [(NumberGauge, ["packet", "query"]),
                          (SetGauge, ["packet", "qtype"])]"""
        super(BundlerFactory, self).__init__()
        self.gauges = gauges

    def build(self):
        """Create a new instance of the bundler."""
        # Instantiate a set of the counters.
        gauges = [klass(params) for klass, params in self.gauges]

        # Return a bundler of the gauges, so the could updated
        # simultaneously.
        return Bundler(gauges)
