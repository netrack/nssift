import bz2
import glob
import os


class BzLoader(object):
    """BZip2 archives loader."""

    # Define a file extension that we should search for
    # in the provided directory.
    file_extension = "*.bz2"

    def __init__(self, filename):
        """Create a new instance of the bzip2 file loaded.

        filename: A path to the file to read."""
        super(BzLoader, self).__init__()
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

        # The implementation is pretty dumb, but we cross the
        # fingers it will work faster then using the regular
        # expressions.
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

        if chunklines:
            yield "\n".join(chunklines)

    def load(self, symbol):
        """Generator of the stripped chunks from the bzip2 archive
        divided by the specified string.

        symbol: A symbol to split the chunks."""
        #print bz2.BZ2File, bz2.BZ2File()
        with bz2.BZ2File(self.filename) as textfile:
            return list(self.isplit(textfile, symbol))
