from config.config import Config
from protocol.server import Server
import logging


def configLogging(level: int):
    logging.basicConfig(
        level=level,
        format="%(asctime)s\t%(levelname)s\t%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler("gateway.log"), logging.StreamHandler()],
    )
    logging.info("Logging configured")


if __name__ == "__main__":
    con = Config()
    configLogging(con.log_level)
    sv = Server(con.host, con.port, con.backlog)
    sv.run()

