import unittest
import unittest.mock

from nssift.netstats.bundler import Bundler
from nssift.netstats import gauge


class TestBundler(unittest.TestCase):
    """Validate the bunlded updates of the statics counters."""

    def _makemock(self, value):
        m = unittest.mock.Mock()
        m.normalize = unittest.mock.Mock(return_value=value)
        return m

    def test_normalize(self):
        gauges = [self._makemock(1.5),
                  self._makemock(3.2),
                  self._makemock(2.8)]

        bundler = Bundler(gauges)
        normalized = list(bundler.normalize())

        # Ensure the normalized returned values.
        self.assertEqual(normalized, [1.5, 3.2, 2.8])

        for gauge in gauges:
            gauge.normalize.assert_called_once_with()
