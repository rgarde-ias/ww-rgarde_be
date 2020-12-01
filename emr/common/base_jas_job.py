"""Base JAS job module."""

from .pmi_spark_job import PMISparkJob


class BaseJASJob(PMISparkJob):
    """Base class for partner-specific JAS aggegation job implementations.

    Parameters
    ----------
    name
        Job name as displayed in EMR.
    partner
        The partner.

    Attributes
    ----------
    partner
        The partner.

    """

    def __init__(self, name: str, partner: str):
        super().__init__(name)

        self.partner = partner

        self.add_argument('--data-date', required=True)
        self.add_argument('--data-date-tz-offset', type=int, required=True)
        self.add_argument('--input-database-name', required=True)
        self.add_argument('--output-bucket-name', required=True)
        self.add_argument('--output-bucket-data-prefix', required=True)
