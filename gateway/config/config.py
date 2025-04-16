import os
import logging

class Config:
    def __init__(self):
        self.host = os.getenv("HOST", "")

        log_level = os.getenv("LOG_LEVEL", "DEBUG").upper()
        self.log_level = getattr(logging, log_level) or logging.DEBUG

        try:
            self.port = int(os.getenv("PORT", "9090"))
        except ValueError as e:
            raise ValueError("the given port is not a number") from e

        try:
            self.backlog = int(os.getenv("BACKLOG", "0"))
        except ValueError as e:
            raise ValueError("the given backlog is not a number") from e
