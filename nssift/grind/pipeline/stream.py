import abc
import six


class Stream(metaclass=abc.ABCMeta):
    """Define an interface for the RDD pipelines.

    The derived classes will implements the particular processing of the RDDs.
    """

    def nonefilter(self, value):
        """True if the value is not equal to None and False otherwise."""
        return value is not None

    @abc.abstractmethod
    def launch(self, sc, rdd, params):
        """Launch the stream processing of the optionally specified RDD.

        This method should be overridden in th derived classes to process a
        dataset.

        sc:     A spark context instance.
        rdd:    A resilient distribution dataset.
        params: A dictionary with a shared set parameters."""
