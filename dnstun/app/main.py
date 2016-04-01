import argparse
import logging
import operator
import sys

import pyspark

from dnstun.app.cluster import Cluster


appname = "DNS tunneling detection tool."


def main():
    logging.basicConfig(
        stream=sys.stdout,
        level=logging.INFO)

    parser = argparse.ArgumentParser(appname)
    parser.add_argument(
        "-s", "--source-path-name", required=True,
        help="A path to the directory with bzipped DNS traffic dumps.")
    parser.add_argument(
        "-d", "--destination-path-name",
        help="A path to the output statistics calculation.")

    args = parser.parse_args()

    sc = pyspark.SparkContext(appName=appname)
    cluster = Cluster()
    cluster.launch(sc, args)
    sc.stop()


if __name__ == "__main__":
    main()
