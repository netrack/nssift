import logging
import operator

from dnstun.fileutil.bzloader import BzLoader

LOG = logging.getLogger(__name__)


class Cluster(object):
    """Parallel clustering of DNS traffic."""

    # Define a splitting string that will be used to divide
    # the DNS dump files into the chunks.
    splitstring = "---"

    def __init__(self):
        """Define a new instance of the cluster context.

        context: An instance of the Spark Context."""
        #self.sc = context

    def uncompress(self, filename):
        """Generator of the compressed DNS request/response
        chunks.

        filename: A compressed DNS dump."""
        return BzLoader(filename).load(self.splitstring)

    def launch(self, sc, params):
        """Perform actual computations."""
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

        length_rdd = filechunks_rdd.map(lambda s: len(s))
        count = length_rdd.reduce(operator.add)

        print count
