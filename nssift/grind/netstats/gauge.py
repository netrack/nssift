import abc
import logging
import math
import six


LOG = logging.getLogger(__name__)


class Gauge(metaclass=abc.ABCMeta):
    """Define an interface for the statistics counter.

    Each derived class should implement an update method
    to recalculate (or adjust) the value of the counter."""

    def __init__(self, keys=None):
        """Create a new instance of the gauge.

        keys: A list of nested keys to extract the required data.
        """
        self.keys = keys or []
        self.processed = 0.0
        self.accumulator = 0.0

    def updateall(self, params):
        """Adjust the gathered statistics with the provided
        list of parameters."""
        # Simply call the update for each element of the list.
        list(map(self.update, params))

    @abc.abstractmethod
    def update(self, params):
        """Adjust the gathered statistics with the
        provided parameters.

        This method should be implemented in the
        derived classes."""

    @abc.abstractmethod
    def normalize(self):
        """Adjust the final result to be normalized according
        to the count of collected events.

        This method should be implemented in the derived
        classes."""

    def join(self, other):
        """Adjust the internal counters of the gauge by the
        other gauge of the same type.

        This method could be implemented in the derived
        classes."""
        # Most of the gauge implementations will only need
        # sum the accumulator and processed count.
        self.processed += other.processed
        self.accumulator += other.accumulator

        # Return the reference to the self, so the join
        # operations could be nested.
        return self

    def get(self, params, keys):
        """Retrieve a value from the specified dictionary
        by the nested list of keys."""

        # Iterate over a list of nested dictionaries.
        for key in keys:
            try:
                params = params[key]
            except Exception:
                LOG.debug(
                    "Failed to adjust the %(klass)s, because "
                    "the provided dictionary does no contain a "
                    "nested key: %(key)s" % {
                        "klass": self.__class__.__name__,
                        "key": ".".join(self.keys)})
                return None
        return params

    def quotient(self, numerator, denominator):
        """The quotient of numerator over denominator. Zero, if the
        denominator is equals to zero."""
        return (numerator / denominator if denominator else 0.0)


class ShannonEntropyGauge(Gauge):
    """Define a Shannon entropy computer.

    This gauge is used to calculate the entropy of
    the provided string (in our particular case such
    strings will represent a DNS host name)."""

    def entropy(self, string):
        """A Shannon entropy of the provided string."""
        # Make a simple optimization to calculate the
        # length of the string only once.
        strlen = len(string)

        # Get the probability of characters in the provided string.
        frequencies = (
            float(string.count(c)) / strlen
            for c in set(string))

        # Calculate the entropy.
        return -sum((
            probability * math.log(probability, 2.0)
            for probability in frequencies))

    def update(self, params):
        """Update the entropy gauge of the DNS requests."""
        string = self.get(params, self.keys)
        if string is None:
            return

        self.processed += 1.0
        self.accumulator += self.entropy(string)

    def normalize(self):
        """Normalize the final entropy value by dividing it
        by count of processed updates."""
        return self.quotient(self.accumulator, self.processed)


class NumberGauge(Gauge):
    """Integer gauge used to accumulate number-aware
    statistics."""

    def update(self, params):
        """Adjust the counters of the gauge by processing
        a specified value."""
        value = self.get(params, self.keys)
        if value is None:
            return

        self.processed += 1.0
        self.accumulator += value

    def normalize(self):
        """Normalize the gauge result by dividing the
        accumulator value on count of processed events."""
        return self.quotient(self.accumulator, self.processed)


class SetGauge(Gauge):
    """Set gauge used to accumulate distinct values."""

    def __init__(self, keys=None):
        """Create a new instance of the """
        super(SetGauge, self).__init__(keys)
        self.accumulator = set()

    def update(self, params):
        """Update the set gauge by adding the value to
        the set of elements."""
        value = self.get(params, self.keys)
        if value is None:
            return

        self.processed += 1.0
        self.accumulator.add(value)

    def normalize(self):
        """Normalize the set gauge. This method will simply
        return the count of distinct processed values."""
        return len(self.accumulator)

    def join(self, other):
        """Update the internal counters with the values
        of the other set gauge."""
        self.processed += other.processed
        self.accumulator.union(other.accumulator)

        # Return the reference to the self, so the join
        # operations could be nested.
        return self


class IncrementGauge(Gauge):
    """Incremental gauge used to calculate the count of
    processed elements."""

    def update(self, params):
        """Adjust the counter by one on each call
        of the update method."""
        self.accumulator += 1.0

    def normalize(self):
        """The count of processed events."""
        return self.accumulator
