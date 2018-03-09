import unittest

from nssift.dissect.dig import Dig


class TestDig(unittest.TestCase):
    """Validate the dig dissector."""

    def setUp(self):
        super(TestDig, self).setUp()
        self.dig = Dig()

    def test_basic_dissect(self):
        result = self.dig.dissect("""
            ;; ->>HEADER<<- opcode: QUERY, rcode: NOERROR, id: 28048
            ;; flags: qr aa; QUERY: 1, ANSWER: 1, AUTHORITY: 2, ADDITIONAL: 2

            ;; QUESTION SECTION:
            ;130.152.91.195.in-addr.arpa. IN PTR

            ;; ANSWER SECTION:
            130.152.91.195.in-addr.arpa. 86400 IN PTR h-195.ln.rinet.ru.

            ;; AUTHORITY SECTION:
            152.91.195.in-addr.arpa. 86400 IN NS ns.rinet.ru.
            152.91.195.in-addr.arpa. 86400 IN NS ns.cronyx.ru.

            ;; ADDITIONAL SECTION:
            ns.rinet.ru. 864000 IN A 195.54.192.33
            ns.cronyx.ru. 86400 IN A 158.250.0.62
            """)

        header = result.get("header")

        self.assertEqual(
            header.get("opcode"), "QUERY")
        self.assertEqual(
            header.get("rcode"), "NOERROR")
        self.assertEqual(
            header.get("id"), "28048")

        flags = result.get("flags")
        self.assertEqual(
            flags.get("flags"), "qr aa")
        self.assertEqual(
            flags.get("QUERY"), "1")
        self.assertEqual(
            flags.get("ANSWER"), "1")
        self.assertEqual(
            flags.get("AUTHORITY"), "2")
        self.assertEqual(
            flags.get("ADDITIONAL"), "2")

        self.assertEqual(
            result.get("question_section"),
            ";130.152.91.195.in-addr.arpa. IN PTR")

        self.assertEqual(
            result.get("answer_section"),
            "130.152.91.195.in-addr.arpa. 86400 IN PTR h-195.ln.rinet.ru.")

        self.assertEqual(
            result.get("authority_section"),
            """152.91.195.in-addr.arpa. 86400 IN NS ns.rinet.ru.
            152.91.195.in-addr.arpa. 86400 IN NS ns.cronyx.ru.""")

        self.assertEqual(
            result.get("additional_section"),
            """ns.rinet.ru. 864000 IN A 195.54.192.33
            ns.cronyx.ru. 86400 IN A 158.250.0.62""")
