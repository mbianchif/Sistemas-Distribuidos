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

        # ID
        self.id = 0 # There's only one gateway

        # INPUT_EXCHANGE_NAME
        self.inputExchangeName = os.getenv("INPUT_EXCHANGE_NAME", "")
        if len(self.inputExchangeName) == 0:
            raise ValueError("the input exchange name was not provided")
        
        # INPUT_QUEUE_NAME
        self.inputQueueName = os.getenv("INPUT_QUEUE_NAME", "")
        if len(self.inputQueueName) == 0:
            raise ValueError("the input queues were not provided")

        # INPUT_COPIES
        self.inputCopies = []
        inputCopiesString = os.getenv("INPUT_COPIES", "")
        for copies in inputCopiesString.split(","):
            try:
                self.inputCopies.append(int(copies))
            except ValueError as e:
                raise ValueError("invalid input copy field") from e

        # OUTPUT_EXCHANGE_NAME
        self.outputExchangeName = os.getenv("OUTPUT_EXCHANGE_NAME", "")
        if len(self.outputExchangeName) == 0:
            raise ValueError("the output exchange name was not provided")

        # OUTPUT_QUEUE_NAMES
        outputQueueNamesString = os.getenv("OUTPUT_QUEUE_NAMES", "")
        if len(outputQueueNamesString) == 0:
            raise ValueError("the output queues were not provided")
        self.outputQueueNames = outputQueueNamesString.split(",")

        # OUTPUT_COPIES
        self.outputCopies = []
        outputCopiesString = os.getenv("OUTPUT_COPIES", "")
        for copies in outputCopiesString.split(","):
            try:
                self.outputCopies.append(int(copies))
            except ValueError as e:
                raise ValueError("invalid output copy field") from e
        if len(self.outputQueueNames) != len(self.outputCopies):
            raise ValueError(f"the length of the output queue names and output copies don't match (names: {len(self.outputQueueNames)}, copies: {len(self.outputCopies)})")

