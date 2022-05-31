from abc import ABC, abstractmethod
import pandas as pd

from appraisal import AbstractBaseStrategy


class IMissingDataStrategy(AbstractBaseStrategy):
    """
    Interface for implementing missing data mechanisms with strategy pattern
    """
    @abstractmethod
    def execute(self, attribute, missing_rate) -> None:
        pass


class MCAR(IMissingDataStrategy):
    """
    MCAR mechanism strategy
    """

    def execute(self, column: pd.Series, missing_rate: float) -> pd.Series:
        """
        Erase values from a series with MCAR pattern
        """
        return column.sample(frac=1-missing_rate)


class MAR(IMissingDataStrategy):
    """
    MAR mechanism strategy
    """

    def execute(self, column: pd.Series, missing_rate: float) -> pd.Series:
        """
        Erase values from a series with MAR pattern
        """
        raise NotImplementedError


class NMAR(IMissingDataStrategy):
    """
    NMAR mechanism strategy
    """

    def execute(self, column: pd.Series, missing_rate: float) -> pd.Series:
        """
        Erase values from a series with NMAR pattern
        """
        raise NotImplementedError
