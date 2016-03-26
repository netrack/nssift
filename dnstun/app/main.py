import argparse


def main():
    parser = argpare.ArgumentParser(
        "DNS tunneling detection tool.")

    parser.add_argument(
        "-s", "--src-path", required=True,
        help="A path to the directory with bzipped DNS traffic dumps.")

    parser.add_argument(
        "-d", "--dst-path",
        help="A path to the output statistics calculation.")

    args = parser.parse_args()


if __name__ == "__main__":
    main()
