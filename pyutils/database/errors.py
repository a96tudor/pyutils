from typing import Optional

from pyutils.helpers.errors import Error


class DatabaseError(Error):
    _extension_details = {
        "category": "server",
        "code": "DatabaseError",
        "severity": "error",
    }

    def __init__(self, message: str, engine: Optional[str] = None, *args, **kwargs):
        super().__init__(f"DatabaseError using {engine}: {message}", *args, **kwargs)
