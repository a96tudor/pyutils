import json
import logging
import traceback
from datetime import datetime, timezone


class JSONLogFormatter(logging.Formatter):
    """
    A log formatter that formats log records as JSON strings.
    """

    def format(self, record):
        log = {
            "timestamp": f"{datetime.now(timezone.utc).isoformat()}Z",
            "level": record.levelname,
            "message": record.getMessage(),
            "module": record.module,
            "function": record.funcName,
            "logger": record.name,
        }

        # Optional fields
        for field in ("execution_id", "user_id", "request_id"):
            if hasattr(record, field):
                log[field] = getattr(record, field)

        # Add stack trace if exception is attached
        if record.levelno >= logging.ERROR and record.exc_info:
            log["stack"] = "".join(traceback.format_exception(*record.exc_info))[
                :2000
            ]  # cap at 2KB

        return json.dumps(log)
