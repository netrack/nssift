import logging

from nssift.grind.dissect.dnsdump import DnsDump
from nssift.grind.fileutil.bzloader import BzLoader
from nssift.grind.pipeline import stream


LOG = logging.getLogger(__name__)


class DissectionStream(stream.Stream):
    """Define a stream to dissect the DNS dumps.
    Search for the DNS dump archives and parse theirs content."""

    # Define a splitting string that will be used to divide
    # the DNS dump files into the chunks.
    splitstring = "---"

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

    def launch(self, sc, rdd, params):
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
