import unittest

from nssift.grind.netstats import gauge


class TestGauge(unittest.TestCase):
    """Validate the gauges."""

    def setUp(self):
        super(TestGauge, self).setUp()
        self.Gauge = type(
            "GaugeMock", (gauge.Gauge,),
            {"normalize": lambda self: None,
             "update": lambda self, params: None})

    def test_gauge_nested_getter(self):
        # Define a counter with a deeply nested key.
        keys = ["this", "is", "a", "nested", "key"]
        counter = self.Gauge(keys)

        # Try to retrieve the value from the empty dictionary,
        # and ensure that None is returned.
        self.assertIsNone(counter.get({}, keys))

        # Retrieve the value from the deeply nested dictionary.
        params = {"this": {"is": {"a": {"nested": {"key": "value"}}}}}
        value = counter.get(params, keys)

        # Ensure that correct values is returned.
        self.assertEqual(value, "value")

    def test_gauge_join(self):
        # Define a Gauge mock.
        counter_first = self.Gauge()
        counter_second = self.Gauge()

        counter_first.accumulator = 4.8
        counter_first.processed = 5.0

        counter_second.accumulator = 3.2
        counter_second.processed = 4.0

        # Join the counters together and ensure the updated
        # internal parameters.
        counter_first.join(counter_second)

        self.assertAlmostEqual(counter_first.accumulator, 8.0, places=1)
        self.assertAlmostEqual(counter_first.processed, 9.0, places=1)

    def test_shannon_entropy_gauge(self):
        # Define the entropy counter of the DNS packet names.
        counter = gauge.ShannonEntropyGauge(
            ["query", "packet", "qname"])

        # Validate that for zero processed updates, 0.0 is returned.
        self.assertEqual(counter.normalize(), 0.0)

        # Update the counter twice to validate the result normalization.
        counter.update({"query": {"packet": {"qname": "a.google.com"}}})
        counter.update({"query": {"packet": {"qname": "b.google.com"}}})
        # Update one with a single invalid dictionary, to ensure it
        # is not affecting the final result.
        counter.update({"invalid": {}})

        # Ensure the counter updates.
        self.assertEqual(counter.processed, 2)
        # Validate precise values of the non-normalized entropy.
        self.assertAlmostEqual(counter.accumulator, 5.710, places=2)
        # Validate the normalized result.
        self.assertAlmostEqual(counter.normalize(), 2.855, places=2)

    def test_number_gauge(self):
        # Define a simple accumulator.
        counter = gauge.NumberGauge(
            ["query", "packet", "size"])

        # Ensure that zero is returned for no updates.
        self.assertEqual(counter.normalize(), 0.0)

        # Update the counter twice.
        counter.update({"query": {"packet": {"size": 7.0}}})
        counter.update({"query": {"packet": {"size": 3.0}}})

        # Perform single incorrect update.
        counter.update({"invalid": {"value": 4.5}})

        self.assertEqual(counter.processed, 2)
        self.assertAlmostEqual(counter.accumulator, 10.0, places=1)

        # Ensure the normalized result.
        self.assertAlmostEqual(counter.normalize(), 5.0, places=1)

    def test_set_gauge(self):
        # Define a set gauge.
        counter = gauge.SetGauge(["response", "type"])

        # Ensure zero value is returned.
        self.assertEqual(counter.normalize(), 0.0)

        # Update the counter with some valid testing data.
        counter.update({"response": {"type": "A"}})
        counter.update({"response": {"type": "A"}})
        counter.update({"response": {"type": "MX"}})

        # Update the counter with invalid data, so it should
        # not affect the result.
        counter.update({"should": {"be": "invalid"}})

        # Validate the count or processed updates.
        self.assertEqual(counter.processed, 3.0)
        self.assertEqual(counter.accumulator, set(("A", "MX")))

        # Validate the normalized result.
        self.assertAlmostEqual(counter.normalize(), 2.0, places=1)

    def test_increment_gauge(self):
        # Define an increment gauge.
        counter = gauge.IncrementGauge()

        # Ensure zero value is returned.
        self.assertEqual(counter.normalize(), 0.0)

        # This type of the gauge is not aware of the update
        # content, it is simply counts them.
        counter.update({"request": {"count": 2.5}})
        counter.update({"query": {"qtype": "A"}})

        # Ensure correct counts returned.
        self.assertEqual(counter.normalize(), 2.0)
