import bz2
import glob
import os


class BZipLoader(object):
    """BZip2 archives loader."""

    # Define a file extension that we should search for
    # in the provided directory.
    file_extension = "*.bz2"

    def __init__(self, filename):
        """Create a new instance of the bzip2 file loaded.

        filename: A path to the file to read."""
        self.filename = filename

    @classmethod
    def isearch(cls, path):
        """Search a bzip2 files in the specified directory. Return
        a generator of the file names.

        path: A path to search for bzip2 archives."""
        pattern = os.path.join(path, cls.file_extension)
        return glob.iglob(pattern)

    def isplit(self, textfile, symbol):
        """Split the specified file into the chunks splitted
        by the specified split string."""
        chunklines = []

        for line in textfile:
            line = line.strip()

            # Skip the empty lines from processing.
            if not line:
                continue

            if line == symbol:
                # Return the chunk of the file, that is splitted by
                # the specified string.
                yield "\n".join(chunklines)

                # Clear the chunk array and continue
                # processing the file.
                chunklines = []
                continue

            # Seems like it is working a bit faster than an
            # append call.
            chunklines.extend((line,))

    def iload(self, symbol):
        """Generator of the stripped chunks from the bzip2 archive
        splitted by the specified string.

        symbol: A symbol to split the chunks."""
        with bz2.BZ2File(self.filename) as textfile:
            return self.isplit(textfile)
