from argparse import ArgumentParser, ArgumentDefaultsHelpFormatter, ArgumentTypeError
import pandas as pd
import importlib
from typing import Union
from Strategy.MissingDataStrategy import IMissingDataStrategy, MCAR, NMAR, MAR
from utils import Logging
from typing import Optional


class Eraser:
    def __init__(
        self,
        input_file: str,
        output_file: str,
        attribute: str,
        missing_rate: float == 0.3,
        strategy: IMissingDataStrategy,
        logging: Union[Logging, Optional[None]] = Logging(),
    ) -> None:
        """
        Reads data from a csv 'input_file' and erase values from 'attribute' column using a missing data 'mechanism' at a 'missing_rate'
        Outputs the result to a csv 'output_file' file
        """
        self.strategy = strategy
        self.input_file = input_file
        self.output_file = output_file
        self.attribute = attribute
        self.missing_rate = missing_rate
        self.logging = logging
        self._result = None

    def run(self):
        data = pd.read_csv(self.input_file)
        data[self.attribute] = self.strategy.execute(
            data[self.attribute], self.missing_rate
        )
        self._result = data

    def save_result(self, output_file: Optional[str] = None) -> None:
        """_summary_

        Args:
            name (Optional[str], optional): _description_. Defaults to None.

        Raises:
            Exception: _description_
        """
        if self._result.empty:
            raise Exception("unhandled exception.")

        self._result.to_csv(self.handle_output_file_name(output_file), index=False)
        print(
            f"Arquivo {self.handle_output_file_name(self.output_file)} gerado com sucesso"
        )

    def handle_output_file_name(self, output_file) -> str:
        """
        Checks the output_file extension. Adds '.csv' if not specified.
        """
        if ".csv" in output_file:
            return output_file
        else:
            return f"{output_file}.csv"


def missing_rate_float(x: float) -> float:
    """
    Custom type function for validating the "missing_range" argument
    """
    try:
        x = float(x)
    except ValueError:
        raise ArgumentTypeError("%r not a floating-point literal" % (x,))

    if x < 0.0 or x > 1.0:
        raise ArgumentTypeError("%r not in range [0.0, 1.0]" % (x,))
    return x


# def str_to_class(
#     module_name: str, class_name: str
# ) -> Union[IMissingDataStrategy, None]:
#     """
#     Return a class instance from a string reference
#     """
#     try:
#         module_ = importlib.import_module(module_name)
#         try:
#             class_ = getattr(module_, class_name)()
#         except AttributeError:
#             print("Class does not exist")
#     except ImportError:
#         print("Module does not exist")
#     return class_ or None


def main():
    """
    Main function
    """
    # Parse command line arguments
    parser = ArgumentParser(formatter_class=ArgumentDefaultsHelpFormatter)
    parser.add_argument("-i", "--input_file", help="Name of the input file")
    parser.add_argument("-o", "--output_file", help="Name of the output file")
    parser.add_argument(
        "-a", "--attribute", help="Name of the attribute to erase values"
    )
    parser.add_argument(
        "-m",
        "--mechanism",
        default="MCAR",
        choices=("MCAR", "MAR", "NMAR"),
        help="Missing data mechanism to be applied",
    )
    parser.add_argument(
        "-r",
        "--missing_rate",
        type=missing_rate_float,
        default=0.3,
        help="The rate in which the attribute values will be erased",
    )
    args = vars(parser.parse_args())

    input_file = args["input_file"]
    output_file = args["output_file"]
    attribute = args["attribute"]
    mechanism = args["mechanism"]
    missing_rate = args["missing_rate"]

    strategy = None

    if mechanism.upper() == "MCAR":
        strategy = MCAR()
    else:
        raise Exception("")

    eraser = Eraser(input_file, output_file, attribute, strategy, missing_rate)
    eraser.run()
    eraser.save_result()


if __name__ == "__main__":
    main()
