from typing import Optional, Union
import logging


class Logging:
    ...


class Csv:
    def __init__(
        self, name: str, mode: str, debugger: Union[Logging, Optional[None]] = Logging()
    ) -> None:
        ...
        self.file = open(name, mode)

    def __enter__(self):
        return self.file

    def __exit__(self, exc_type, exc_value, traceback):
        self.file.close()
