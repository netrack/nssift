import argparse
import itertools
import re
import math


class DnsParser(object):

    request_attributes = {
        "query_ip": re.compile(r"query_ip: ([\d:\.\w]+)"),
        "response_ip": re.compile(r"response_ip: ([\d:\.\w]+)"),
        "qtype": re.compile(r"qtype: (\w+).*"),
        "qname": re.compile(r"qname: (.*)"),
        "query": re.compile(r"query: \[(\d+) octets\]"),
    }

    response_attributes = {
        "response": re.compile(r"response: \[(\d+) octets\]"),
        "rcode": re.compile(r";; ->>HEADER<<- opcode: \w+, rcode: (\w+), .*"),
    }

    def __init__(self, filepath):
        super(DnsParser, self).__init__()
        self.filepath = filepath
        self.statistics = []

    def load(self):
        with open(self.filepath) as dnstraffic:
            filecontent = dnstraffic.read()

        index, queries = 0, filecontent.split("---\n")

        while index < len(queries):
            reqstat, resstat = self.parse_request(queries[index]), {}

            if (index+1 < len(queries) and
                    "response:" in queries[index+1] and
                    "PARSE ERROR" not in queries[index+1]):
                index += 1
                resstat = self.parse_response(queries[index])

            index += 1

            if reqstat:
                self.statistics.append((reqstat, resstat))

    def parse_request(self, request):
        request = request.strip()
        attrs = {}

        if not request:
            return None

        for attr, regexp in self.request_attributes.iteritems():
            search = regexp.search(request)
            if not search:
                return None

            attrs.update({attr: search.group(1)})

        return attrs

    def parse_response(self, response):
        response = response.strip()
        attrs = {}

        if not response:
            return attrs

        for attr, regexp in self.response_attributes.iteritems():
            search = regexp.search(response)
            if not search:
                return None

            attrs.update({attr: search.group(1)})

        return attrs

    def shannon_entropy(self, string):
        # get probability of chars in string
        prob = [float(string.count(c)) / len(string)
                for c in dict.fromkeys(list(string))]
        # calculate the entropy
        entropy = - sum([p * math.log(p) / math.log(2.0) for p in prob])
        return entropy

    def dump(self):
        hosts = {}

        # - Average request packet size
        # - Average response packet size
        # - Hostname entropy
        # - Count of DNS packets per record types
        # - Percentage of packets without response
        for req, res in self.statistics:
            query_ip = req.get("query_ip")

            if query_ip not in hosts:
                hosts.update({query_ip: {
                    "requests": 0,
                    "responses": 0,
                    "transmitted": 0,
                    "received": 0,
                    "entropy": 0,
                    "types": set()}})

            hosts[query_ip]["requests"] += 1
            hosts[query_ip]["transmitted"] += int(req["query"])
            hosts[query_ip]["entropy"] += self.shannon_entropy(req["qname"])
            hosts[query_ip]["types"].add(req["qtype"])

            if not res or "NOERROR" not in res["rcode"]:
                continue

            hosts[query_ip]["responses"] += 1
            hosts[query_ip]["received"] += int(res["response"])

        for query_ip, stats in hosts.iteritems():
            stats["types"] = len(stats["types"])
            stats["entropy"] /= float(stats["requests"])
            stats["transmitted"] /= float(stats["requests"])

            if stats["received"] and stats["responses"]:
                stats["received"] /= float(stats["responses"])

            stats["errors"] = 1 - (
                float(stats["responses"]) /
                stats["requests"])

        return hosts


def savedata(filename, stats):
    with open(filename, "w") as filestat:
        for stat in stats.itervalues():
            del stat["responses"]
            point = "\t".join(map(str, stat.values()))
            filestat.write("%(point)s\n" % {"point": point})


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-i", "--input-file",
        help="Path to the DNS file queries dump.",
        required=True)

    parser.add_argument(
        "-o", "--output-file",
        help="Path to the destination statistics.",
        required=True)

    args = parser.parse_args()

    dnsparser = DnsParser(args.input_file)
    dnsparser.load()
    savedata(args.output_file, dnsparser.dump())


if __name__ == "__main__":
    main()
