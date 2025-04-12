import os

class Config:
    def __init__(self):
        self.host = os.getenv("HOST")
        if not self.host:
            self.host = ""
        
        port_var = os.getenv("PORT")
        if not port_var:
            raise ValueError("no port was given")

        try:
            self.port = int(port_var)
        except ValueError as e:
            raise ValueError("the given port is not a number") from e

