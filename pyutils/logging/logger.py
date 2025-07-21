from logging import INFO, Formatter, Handler
from logging import Logger as Logger_
from typing import Optional, Union

from pyutils.decorators.singleton import singleton


@singleton
class Logger(Logger_):
    def __init__(
        self,
        name: str,
        formatter: Formatter,
        handler: Handler,
        level: Optional[Union[int, str]] = INFO,
        *args,
        **kwargs,
    ):
        super().__init__(name, *args, **kwargs)
        self.setLevel(level)
        handler.setFormatter(formatter)
        self.addHandler(handler)
        self.propagate = False
        self.formatter = formatter
