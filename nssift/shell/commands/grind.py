import pyspark

import nssift.shell

from nssift.grind.cluster import Cluster
from nssift.grind.cluster import streams



class Grind(nssift.shell.Command):
    """Grind is a command to run Spark job that aggregates statistics of the
    DNS traffic."""

    name = "grind"
    aliases = ["g"]
    help = "collect DNS traffic statistic"

    arguments = [
        (["-s", "--source-path"],
         dict(metavar="SOURCE",
              help="A path to directory with DNS traffic archives",
              required=True)),

        (["-c", "--clusters"],
         dict(metavar="CLUSTERS",
              help="Count of cluster to divide data",
              required=True,
              type=int)),

        (["-d", "--destination-path"],
         dict(metavar="DESTINATION",
              help="A path to the output statistic.",
              required=True)),

        (["-r", "--render-plot"],
         dict(action="store_true",
              help="Render an image of clustered data.")),
    ]

    def handle(self, context):
        """Launch Spark job to parse DNS traffic."""
        sc = pyspark.SparkContext(appName="nssift")
        cluster = Cluster(streams=streams())
        cluster.launch(sc, context.args)
        sc.stop()
