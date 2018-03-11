import datetime
import itertools
import numpy
import logging

from matplotlib import pyplot
from mpl_toolkits.mplot3d import Axes3D
from pyspark.mllib.clustering import KMeans

from nssift.grind.pipeline import stream


LOG = logging.getLogger(__name__)


class ClusteringStream(stream.Stream):
    """Define the clustering stream."""

    # Set the destination image DPI.
    dpi = 600

    # Color used to render the cluster centers.
    centers_color = "#d32f2f"

    # Color used to render the data points.
    points_color = "#727272"

    def array(self, value):
        """Translate the result of statistics collection to the numpy array."""
        _, bundler = value
        return numpy.array(bundler.normalize())

    def render_textfile(self, filename, points):
        """Render the specified list of points into the empty file."""
        with open(filename, "w") as textfile:
            # Write the multi-dimensional point into the
            # separate line.
            for point in points:
                line = " ".join(map(str, point))
                textfile.write("%(line)s\n" % {"line": line})

    def render_text(self, filename, centers, points):
        """Render the centers and points into the file.

        centers: An array of the cluster centers.
        points:  An array of the host statistics."""
        centers_filename = "centers-%(filename)s" % {
            "filename": filename}

        self.render_textfile(filename, points)
        self.render_textfile(centers_filename, centers)

    def render_plot(self, centers, points):
        """Render the clusters into the PNG image.

        centers: An array of the cluster centers.
        points:  An array of the host statistics."""
        figure = pyplot.figure()
        figure.set_dpi(self.dpi)

        # Convert the specified arrays of the cluster centers
        # and data points into the numpy array, so we could
        # easily draw required plots.
        centers, points = list(map(numpy.array, [centers, points]))

        plot = figure.add_subplot(111, projection="3d")
        plot.scatter(centers[:,0], centers[:,1], centers[:,2],
                     color=self.centers_color)
        plot.scatter(points[:,0], points[:,1], points[:,2],
                     color=self.points_color, alpha=0.4)

        # Define a time format for the destination image
        timefmt = "%d-%a-%Y-%M-%S"
        timestamp = datetime.datetime.now().strftime(timefmt)

        # Define a plot image name with a created time stamp.
        figurename = "nssift-%(timestamp)s.png" % {"timestamp": timestamp}
        LOG.info("Rendering a figure with a cluster centers "
                 "%(figurename)s" % {"figurename": figurename})

        # Render the image with a cluster centers.
        figure.savefig(figurename)

    def launch(self, sc, rdd, params):
        """Cluster the results of DNS dump processing.

        rdd: RDD result of the statistics aggregation."""
        # Convert the aggregated statistics to the points
        # of the n-dimensional pointers.
        stats_rdd = rdd.map(self.array)

        # Produce the clusters from the aggregated statistics.
        clusters = KMeans.train(stats_rdd, params.clusters)
        points = stats_rdd.collect()

        # Render the collected statistics into the specified
        # file name. If the file is not specified the data will
        # be saved into the auto-generated file name.
        #
        # The cluster centers will be written into the file
        # prefixed by the "centers" word.
        self.render_text(
            params.destination_path_name,
            clusters.centers, points)

        # Render a plot with a accumulated statistics and
        # cluster centers.
        if params.render_plot:
            self.render_plot(clusters.centers, points)
