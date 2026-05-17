from logging import INFO, Formatter, Handler
from typing import Union

from .logger import Logger


def get_logger(
    name: str,
    formatter: Formatter,
    handler: Handler,
    level: Union[int, str] = INFO,
    **kwargs,
):
    return Logger(
        name=name,
        formatter=formatter,
        handler=handler,
        level=level,
        **kwargs,
    )
