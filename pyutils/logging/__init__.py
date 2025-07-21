from logging import INFO, Formatter, Handler
from typing import Optional, Union

from .logger import Logger


def get_logger(
    name: str,
    formatter: Formatter,
    handler: Handler,
    level: Optional[Union[int, str]] = INFO,
    *args,
    **kwargs,
):
    return Logger(
        name=name,
        formatter=formatter,
        handler=handler,
        level=level,
        *args,
        **kwargs,
    )
