import argparse
import logging
import operator
import sys

import pyspark

from dnstun.app.cluster import Cluster
from dnstun.app.cluster import streams

appname = "DNS tunneling detection tool."


def main():
    logging.basicConfig(stream=sys.stdout, level=logging.INFO)

    parser = argparse.ArgumentParser(appname)
    parser.add_argument(
        "-s", "--source-path-name", required=True,
        help="A path to directory with bzip2-compressed DNS traffic dumps.")
    parser.add_argument(
        "-c", "--clusters", required=True, type=int,
        help="Count of lusters that data will be divided into.")
    parser.add_argument(
        "-d", "--destination-path-name", required=True,
        help="A path to the output statistics calculation.")
    parser.add_argument(
        "-r", "--render-plot", action="store_true",
        help="Render an image with collected statistics.")

    args = parser.parse_args()

    sc = pyspark.SparkContext(appName=appname)
    cluster = Cluster(streams=streams())
    cluster.launch(sc, args)
    sc.stop()


if __name__ == "__main__":
    main()
