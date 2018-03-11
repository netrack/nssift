import bz2
import glob
import unittest
import unittest.mock

from nssift.grind.fileutil.bzloader import BzLoader


class TestBzLoader(unittest.TestCase):
    """Validate the bzip2 file loading."""

    @unittest.mock.patch.object(glob, "iglob")
    def test_isearch(self, iglob_mock):
        BzLoader.isearch("/mnt/dns/test")
        # Validate the correct search string is crafted.
        iglob_mock.assert_called_once_with("/mnt/dns/test/*.bz2")

    @unittest.mock.patch.object(BzLoader, "isplit")
    def test_load(self, isplit_mock):
        textfile_mock = unittest.mock.MagicMock()

        instance_mock = unittest.mock.MagicMock()
        instance_mock.__enter__ = unittest.mock.MagicMock(
            return_value=textfile_mock)

        # Mock the BZ2File constructor to validate passed arguments.
        bz2file_mock = unittest.mock.MagicMock(return_value=instance_mock)

        with unittest.mock.patch.object(bz2, "BZ2File", bz2file_mock):
            loader = BzLoader("DNS3-20160126_0705-P0001231.pres.bz2")
            loader.load("---")

            # Ensure the correct filename was passed.
            bz2file_mock.assert_called_once_with(
                "DNS3-20160126_0705-P0001231.pres.bz2")

            # Validate the splitting symbol value.
            isplit_mock.assert_called_once_with(textfile_mock, "---")

    def test_split(self):
        loader = BzLoader(None)

        # Validate that the array of strings will be divided
        # by the specified symbol.
        array = ["first", "second", "---", "third", "fourth"]
        splits = list(loader.isplit(array, "---"))

        # Ensure the string array is divided correctly.
        self.assertEqual(splits, ["first\nsecond", "third\nfourth"])

        array = ["first", "", "\nsecond", "---\n", "third", "fourth"]
        splits = list(loader.isplit(array, "---"))

        # Ensure that each line is stripped and empty lines are omitted.
        self.assertEqual(splits, ["first\nsecond", "third\nfourth"])

        array = ["first", "second", "---", "third", "fourth", "---", "fifth"]
        splits = list(loader.isplit(array, "---"))

        # Validate the splitting into the multiple chunks.
        self.assertEqual(splits, ["first\nsecond", "third\nfourth", "fifth"])
