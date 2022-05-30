"""
    Appraisal package
"""


from abc import ABC, abstractmethod
from typing import Optional
from pandas import DataFrame
from appraisal.utils import Logging


class AbstractBaseStrategy(ABC):

    @abstractmethod
    def execute(self, *args, **kwargs):
        """
        Abstract Strategy
        """
        raise NotImplementedError()


class _PipelineTask(object):

    def __init__(
        self,
        input_file: str,
        output_file: str,
        attribute: str,
        strategy: AbstractBaseStrategy,
        logging: Optional[Logging]
    ) -> None:
        self.input_file = input_file
        self.output_file = output_file
        self.attribute = attribute
        self._strategy = strategy
        self.logging = logging

    @property
    def strategy(self):
        return self._strategy

    @strategy.setter
    def strategy(self, value):
        self._strategy = value

    @abstractmethod
    def run(self, *args, **kwargs) -> DataFrame:
        """
            method to run the pipeline Task.

        Raises:
            NotImplementedError: _description_

        Returns:
            DataFrame: _description_
        """
        raise NotImplementedError(
            "Should implement the method 'run' for the Subclass")

    def _handle_output_file_name(self, output_file) -> str:
        """
        Checks the output_file extension. Adds '.csv' if not specified.
        """
        if ".csv" in output_file:
            return output_file
        else:
            return f"{output_file}.csv"

    def save_result(self, result: DataFrame, output_file: Optional[str] = None) -> None:
        """_summary_

        Args:
            name (Optional[str], optional): _description_. Defaults to None.

        Raises:
            Exception: _description_
        """
        if result.empty:
            raise Exception("unhandled exception.")

        result.to_csv(self._handle_output_file_name(output_file), index=False)

        print(
            f"Arquivo {self.handle_output_file_name(output_file)} gerado com sucesso"
        )

    def __call__(self, *args: Any, **kwds: Any) -> Any:
        result = self.run()
        self.save_result(result, self.output_file)
