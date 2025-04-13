from protocol.socket import CsvTransferListener, CsvTransferStream, MSG_BATCH, MSG_FIN, MSG_ERR
from config.config import Config
import logging

def configLogging():
    logging.basicConfig(
        level=logging.DEBUG,
        format="%(asctime)s - %(levelname)s - %(message)s",
        handlers=[
            logging.FileHandler("gateway.log"),
            logging.StreamHandler()
        ]
    )
    logging.info("Logging configured")

def main():
    con = Config()
    configLogging()

    lis = CsvTransferListener.bind(con.host, con.port)

    logging.info(f"Waiting for connections...")
    stream, addr = lis.accept()
    logging.info(f"Got a new connection from {addr}")
    lis.close()
    
    for _ in range(3):
        filename = stream.filename()
        print(f"filename: {filename}")

        while True:
            msg = stream.recv()

            if msg.kind == MSG_BATCH:
                # Enviar el archivo correspondiente a la cola de mensajes
                print(f"recv lines: {len(msg.data)}")
                logging.debug("Read batch")
            elif msg.kind == MSG_FIN:
                logging.info("A file has finished reading")
                break
            elif msg.kind == MSG_ERR:
                logging.critical("An error occurred, exiting...")
                stream.close()
                return
            else:
                logging.critical(f"An unknown msg kind was received {msg.kind}")
                return
    
    logging.info("File upload was successfull")
    
    # quedarse esperando por los resultados para enviarselos al clinte
    stream.close()

if __name__ == "__main__":
    main()