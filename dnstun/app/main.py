import argparse
import operator

import pyspark

from dnstun.fileutil.bzloader import BzLoader


appname = "DNS tunneling detection tool."


def main():
    parser = argparse.ArgumentParser(appname)
    parser.add_argument(
        "-s", "--src-path", required=True,
        help="A path to the directory with bzipped DNS traffic dumps.")
    parser.add_argument(
        "-d", "--dst-path",
        help="A path to the output statistics calculation.")

    args = parser.parse_args()

    sc = pyspark.SparkContext(appName=appname)

    files_rdd = sc.parallelize(BzLoader.isearch(args.src_path))
    length_rdd = files_rdd.map(lambda s: len(s))
    count = length_rdd.reduce(operator.add)

    print(count)
    sc.stop()


if __name__ == "__main__":
    main()
