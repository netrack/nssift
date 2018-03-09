import unittest

from nssift.dissect.proto import Proto


class TestProto(unittest.TestCase):
    """Validate the protocol dissector."""

    def setUp(self):
        super(TestProto, self).setUp()
        self.proto = Proto()

    def test_basic_dissect(self):
        result = self.proto.dissect(
            "type: UDP_QUERY_RESPONSE\n"
            "query_ip: 144.82.39.72\n"
            "response_ip: 46.51.185.157\n")

        self.assertEqual(result.get("type"), "UDP_QUERY_RESPONSE")
        self.assertEqual(result.get("query_ip"), "144.82.39.72")
        self.assertEqual(result.get("response_ip"), "46.51.185.157")

    def test_key_and_values_stripping(self):
        result = self.proto.dissect(
            " qclass : IN (1)  \n"
            " qtype:   TXT (16)\n"
            "rcode :  NXDOMAIN (3) \n")

        self.assertEqual(result.get("qclass"), "IN (1)")
        self.assertEqual(result.get("qtype"), "TXT (16)")
        self.assertEqual(result.get("rcode"), "NXDOMAIN (3)")
