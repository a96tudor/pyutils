from logging import Handler

import requests


class CommandLineHandler(Handler):

    def __init__(self, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.formatter = None

    def emit(self, record):
        """
        Emit a record.
        """
        if self.formatter:
            record.msg = self.formatter.format(record)
        print(record.msg)


class BetterStackHandler(Handler):
    def __init__(self, api_key: str, *args, **kwargs):
        super().__init__(*args, **kwargs)
        self.__api_key = api_key
        self.endpoint = "https://in.logs.betterstack.com"

    @property
    def headers(self) -> dict:
        return {
            "Authorization": f"Bearer {self.__api_key}",
            "Content-Type": "application/json",
        }

    def emit(self, record):
        try:
            payload = self.format(record)
            requests.post(self.endpoint, headers=self.headers, data=payload)
        except Exception as e:
            print(f"[BetterStack] Log send failed: {e}")
