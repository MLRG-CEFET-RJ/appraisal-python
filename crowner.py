from abc import ABC, abstractmethod
from argparse import ArgumentDefaultsHelpFormatter, ArgumentParser
from typing import Optional, Union
from utils import Csv, Logging
import pandas as pd
from pandas import DataFrame
import numpy as np


class AbstractImputationPlan(ABC):
    """
    Abstract base class ImputationPlan.
    All imputation plans needs inherit from this base class.
    """

    @abstractmethod
    def strategy(self, csv: Csv, column_name: str):
        """
        Abstract Strategy
        """
        raise NotImplementedError()


class Mean(AbstractImputationPlan):
    """
    Implement a imputation plan strategy.
    """

    def strategy(self, df: DataFrame, column_name: str) -> DataFrame:
        """_summary_

        Args:
            df (DataFrame): An Pandas dataframe.
            column_name (str): A column name from the dataframe.

        Returns:
            DataFrame: Return the computed dataframe.
        """
        _mean = df[column_name].mean()
        df[column_name] = df[column_name].replace(np.nan, _mean)
        return df


class Crowner:
    """
    Implement the crowner.
    """

    def __init__(
        self,
        input_file: str,
        output_file: str,
        attribute: str,
        strategy: AbstractImputationPlan,
        debugger: Union[Logging, Optional[None]] = Logging(),
    ) -> None:
        self.input_file = input_file
        self.attribute = attribute
        self.strategy = strategy
        self.debugger = debugger
        self._result = None

    def run(self) -> None:
        """
        Execute the Cronwer runner.
        """
        df = pd.read_csv(self.csv)
        self._result = self.strategy.strategy(df, self.attribute)

    def save_result(self, output_file: Optional[str] = None) -> None:
        """_summary_

        Args:
            name (Optional[str], optional): _description_. Defaults to None.

        Raises:
            Exception: _description_
        """
        if self._result.empty:
            raise Exception("unhandled exception.")

        self._result.to_csv(output_file)


# Parse arguments
parser = ArgumentParser()
parser.add_argument("-i", "--input_file", help="Name of the input file")
parser.add_argument("-o", "--output_file", help="Name of the output file")
parser.add_argument(
    "-a", "--attribute", help="Name of the column to fills in the missing values."
)
parser.add_argument(
    "-p",
    "--plan",
    default="mean",
    help="imputation plan to be used. The default value of this is mean",
)
args = vars(parser.parse_args())


def crowner(input_file, output_file, column_name, plan):
    """_summary_

    Args:
        input_file (_type_): input file
        output_file (_type_): output file
        column_name (_type_): column name to be filled.
        plan (_type_): plan to be used.

    Raises:
        Exception: _description_
    """

    strategy = None

    if plan == "mean":
        strategy = Mean()
    else:
        raise Exception("")

    with Csv(input_file, "r") as csv:
        column_name = column_name
        crowner = Crowner(csv, column_name, strategy)
        crowner.run()
        crowner.save_result(output_file)


def main():
    input_file = args["input_file"]
    output_file = args["output_file"]
    attribute = args["attribute"]
    plan = args["plan"]
    crowner(input_file, output_file, attribute, plan)


if __name__ == "__main__":
    main()
