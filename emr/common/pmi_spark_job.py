"""PMI Spark job module."""

from abc import ABC, abstractmethod
import argparse

from pyspark.sql import SparkSession


class PMISparkJob(ABC):
    """Base class for Spark jobs that are part of the PMI pipeline.

    Parameters
    ----------
    name
        Job name as displayed in EMR.

    """

    def __init__(self, name: str):

        self._name = name

        self._args_parser = argparse.ArgumentParser()


    def add_argument(self, *args, **kwargs) -> None:
        """Add command line argument.

        Supposed to be used in the sub-class constructor. The arguments are the
        same as for `ArgumentParser.add_argument()` function.

        """

        self._args_parser.add_argument(*args, **kwargs)


    def run(self) -> None:
        """Run the job."""

        args = self._args_parser.parse_args()

        spark = SparkSession \
            .builder \
            .appName(self._name) \
            .enableHiveSupport() \
            .getOrCreate()
        try:
            self.execute(spark, args)
        finally:
            spark.stop()


    @abstractmethod
    def execute(self, spark: SparkSession, args: argparse.Namespace) -> None:
        """Execute the job logic.

        Parameters
        ----------
        spark
            Spark session to use.
        args
            Arguments provided in the command line.

        """
