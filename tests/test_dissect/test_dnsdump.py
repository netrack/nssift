import unittest

from nssift.dissect.dnsdump import DnsDump


class TestDnsDump(unittest.TestCase):
    """Validate the DNS dump dissection."""

    def setUp(self):
        super(TestDnsDump, self).setUp()
        self.dnsdump = DnsDump()
        self.maxDiff = None

    def test_populate_request(self):
        meta = {"query": 227}
        packet = {"header": {"id": "28048"}}

        # Validate the dictionary produced for the DNS
        # request packet.
        result = self.dnsdump.populate(meta, packet)

        # Define a dictionary format that is expected to be
        # returned from the population routine.
        expected = {"id": "28048", "transaction": [
                    {"type": "REQUEST", "meta": meta,
                     "packet": packet}]}

        self.assertEqual(result, expected)

    def test_populate_response(self):
        meta = {"response": 154}
        packet = {"header": {"id": "11578"}}

        # Validate the dictionary produced for the DNS
        # response packet.
        result = self.dnsdump.populate(meta, packet)

        # Define a dictionary format that is expected to be
        # returned from the population routine.
        expected = {"id": "11578", "transaction": [
                    {"type": "RESPONSE", "meta": meta,
                     "packet": packet}]}

        self.assertEqual(result, expected)

    def test_populate_invalid(self):
        meta = {"query": 192}
        packet = {"header": {"notanid": 32462}}

        # Validate that for incorrectly parsed request,
        # populate routine will return None.
        result = self.dnsdump.populate(meta, packet)
        self.assertIsNone(result)

        meta = {"proto": "TCP"}
        packet = {"header": {"id": 32462}}

        # The parsed metadata should contain either the query
        # size of response size. Otherwise the request will be
        # treated as invalid and None will be returned.
        result = self.dnsdump.populate(meta, packet)
        self.assertIsNone(result)

    def test_fetch_octets(self):
        result = self.dnsdump.fetch_octets(
            {"query": "[187 octets]"}, "query")

        # Validate that result correctly translated
        # to the integer value.
        self.assertEqual(result, {"query": 187})

        result = self.dnsdump.fetch_octets(
            {"response": "[215 bytes]"}, "response")

        # Validate, if the value does not match a regular
        # expression the method will return it as is.
        self.assertEqual(result, {"response": "[215 bytes]"})

        result = self.dnsdump.fetch_octets(
            {"query": "[256 octets]"}, "response")

        # Ensure, the same dictionary will be returned if
        # the specified attribute is not found.
        self.assertEqual(result, {"query": "[256 octets]"})

    def test_dissect_errors(self):
        result = self.dnsdump.dissect(
            """This is pretty unexpected
            piece of the text.""")

        # Validate that None is returned on incorrect
        # chunk of the text.
        self.assertIsNone(result)

        result = self.dnsdump.dissect("""
            type: UDP_QUERY_RESPONSE
            query_ip: 144.82.39.72
            response_ip: 46.51.185.157""")

        # This example is only partial, since the Dig
        # output part is also expected to be specified,
        # therefor it will be treated as incomplete.
        self.assertIsNone(result)

    def test_dissect_request(self):
        result = self.dnsdump.dissect("""
            query_ip: 37.9.72.211
            id: 19254
            qname: 221.160.5.15.in-addr.arpa.
            qtype: PTR (12)
            query: [43 octets]
            ;; ->>HEADER<<- opcode: QUERY, rcode: NOERROR, id: 19254
            ;; flags:; QUERY: 1, ANSWER: 0, AUTHORITY: 0, ADDITIONAL: 0

            ;; QUESTION SECTION:
            ;221.160.5.15.in-addr.arpa. IN PTR

            ;; ANSWER SECTION:

            ;; AUTHORITY SECTION:

            ;; ADDITIONAL SECTION:
            """)

        meta = {"id": "19254",
                "query": 43,
                "qname": "221.160.5.15.in-addr.arpa.",
                "qtype": "PTR (12)",
                "query_ip": "37.9.72.211"}

        header = {"opcode": "QUERY",
                  "id": "19254",
                  "rcode": "NOERROR"}

        flags = {"flags": "",
                 "QUERY": "1",
                 "ANSWER": "0",
                 "AUTHORITY": "0",
                 "ADDITIONAL": "0"}

        packet = {"header": header,
                  "flags": flags,
                  "question_section": ";221.160.5.15.in-addr.arpa. IN PTR",
                  "answer_section": "",
                  "authority_section": "",
                  "additional_section": ""}

        payload = {"type": "REQUEST",
                   "meta": meta,
                   "packet": packet}

        # Compare the result of the DNS request dissection
        # with a standard value.
        self.assertEqual(
            result, {"id": "19254",
                     "transaction": [payload]})

    def test_dissect_response(self):
        result = self.dnsdump.dissect("""
            response: [53 octets]
            ;; ->>HEADER<<- opcode: QUERY, rcode: NOERROR, id: 61168
            ;; flags: qr aa; QUERY: 1, ANSWER: 1, AUTHORITY: 0, ADDITIONAL: 0

            ;; QUESTION SECTION:
            ;rosfirm.ru. IN A

            ;; ANSWER SECTION:
            rosfirm.ru. 86400 IN A 212.23.90.34

            ;; AUTHORITY SECTION:

            ;; ADDITIONAL SECTION:
            """)

        meta = {"response": 53}
        header = {"id": "61168",
                  "opcode": "QUERY",
                  "rcode": "NOERROR"}

        flags = {"flags": "qr aa",
                 "QUERY": "1",
                 "ANSWER": "1",
                 "AUTHORITY": "0",
                 "ADDITIONAL": "0"}

        packet = {"header": header,
                  "flags": flags,
                  "question_section": ";rosfirm.ru. IN A",
                  "answer_section": "rosfirm.ru. 86400 IN A 212.23.90.34",
                  "authority_section": "",
                  "additional_section": ""}

        payload = {"type": "RESPONSE",
                   "meta": meta,
                   "packet": packet}

        # Compare the result of the DNS response dissection
        # with a standard value.
        self.assertEqual(
            result, {"id": "61168",
                     "transaction": [payload]})
