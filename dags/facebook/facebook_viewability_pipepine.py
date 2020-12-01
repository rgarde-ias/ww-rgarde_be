"""Facebook viewability pipeline implementation module."""

from ..common import BaseViewabilityPipeline, EmrCluster

from .constants import PARTNER


class FacebookViewabilityPipeline(BaseViewabilityPipeline):
    """Facebook viewability pipeline implementation.

    Parameters
    ----------
    emr_cluster
        The EMR cluster.

    """

    def __init__(
            self,
            *,
            emr_cluster: EmrCluster
    ):
        super().__init__(
            partner=PARTNER,
            emr_cluster=emr_cluster,
            emr_jas_script='facebook_jas.py',
            emr_mart_script='jas_mart.py',
        )
