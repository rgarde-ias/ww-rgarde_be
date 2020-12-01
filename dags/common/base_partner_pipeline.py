"""Base class for partner pipeline implementations module."""

from abc import ABC, abstractmethod

from airflow.models import BaseOperator


class BasePartnerPipeline(ABC):
    """Base class for partner pipeline implementations."""

    def __init__(self, partner: str):

        self._partner = partner

    @property
    def partner(self) -> str:
        """The partner."""
        return self._partner

    @property
    @abstractmethod
    def entry_task(self) -> BaseOperator:
        """First task in the pipeline."""

    @property
    @abstractmethod
    def exit_task(self) -> BaseOperator:
        """Last task in the pipeline."""
