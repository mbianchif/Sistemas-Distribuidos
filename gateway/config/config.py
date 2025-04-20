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

        self.inputExchangeName = os.getenv("INPUT_EXCHANGE_NAME", "gateway")
        if len(self.inputExchangeName) == 0:
            raise ValueError("the input exchange name was not provided")

        validExchangeTypes = ["direct", "fanout", "topic", "headers"]
        self.inputExchangeType = os.getenv("INPUT_EXCHANGE_TYPE")
        if self.inputExchangeType not in validExchangeTypes:
            raise ValueError(f"the input exchange type is invalid: {self.inputExchangeType}")
        
        self.inputQueueNamesString = os.getenv("INPUT_QUEUE_NAMES")
        if len(self.inputQueueNamesString) == 0:
            raise ValueError("the input queues were not provided")

        self.inputQueueNames = self.inputQueueNamesString.split(",")

        inputQueueKeysString = os.getenv("INPUT_QUEUE_KEYS")
        self.inputQueueKeys = inputQueueKeysString.split(",")
        if len(self.inputQueueNames) != len(self.inputQueueKeys):
            raise ValueError(f"length for input queue names and keys don't match (names: {len(self.inputQueueNames)}, keys: {len(self.inputQueueKeys)})")

        self.outputExchangeName = os.getenv("OUTPUT_EXCHANGE_NAME")
        if len(self.outputExchangeName) == 0:
            raise ValueError("the output exchange name was not provided")
        

        self.outputExchangeType = os.getenv("OUTPUT_EXCHANGE_TYPE")
        if self.outputExchangeType not in validExchangeTypes:
            raise ValueError(f"the output exchange type is invalid: {self.outputExchangeType}")

        outputQueueNamesString = os.getenv("OUTPUT_QUEUE_NAMES")
        if len(outputQueueNamesString) == 0:
            raise ValueError("the output queues were not provided")
        
        self.outputQueueNames = outputQueueNamesString.split(",")

        outputQueueKeysString = os.getenv("OUTPUT_QUEUE_KEYS")
        self.outputQueueKeys = outputQueueKeysString.split(",")
        if len(self.outputQueueNames) != len(self.outputQueueKeys):
        	raise ValueError(f"length for output queue names and keys don't match (names: {len(self.outputQueueNames)}, keys: {len(self.outputQueueKeys)})")

        
