from protocol.socket import CsvTransferListener, MSG_BATCH, MSG_FIN, MSG_ERR
from config.config import Config
import logging


def configLogging(level: int):
    logging.basicConfig(
        level=level,
        format="%(asctime)s\t%(levelname)s\t%(message)s",
        datefmt="%Y-%m-%d %H:%M:%S",
        handlers=[logging.FileHandler("gateway.log"), logging.StreamHandler()],
    )
    logging.info("Logging configured")


def main() -> int:
    con = Config()
    configLogging(con.log_level)

    lis = CsvTransferListener.bind(con.host, con.port)

    logging.info(f"Waiting for connections...")
    stream, addr = lis.accept()
    logging.info(f"Got a new connection from {addr}")
    lis.close()

    for _ in range(3):
        filename = stream.filename()
        logging.info(f"Receiving {filename}")

        while True:
            msg = stream.recv()
            if msg.kind == MSG_FIN:
                logging.info(f"{filename} was successfully received")
                break
            elif msg.kind == MSG_BATCH:
                # Enviar el archivo correspondiente a la cola de mensajes
                pass
            elif msg.kind == MSG_ERR:
                logging.critical("An error occurred, exiting...")
                stream.close()
                return 1
            else:
                logging.critical(f"An unknown msg kind was received {msg.kind}")
                stream.close()
                return 1

    logging.info("Data files were successfully transfered")
    # quedarse esperando por los resultados para enviarselos al clinte

    stream.close()
    return 0


if __name__ == "__main__":
    exit(main())
