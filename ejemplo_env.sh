# .env
RABBIT_URL=amqp://guest@guest.fi.uba.ar

# compose
ID=1
    # debe ser numerico, es tambien el numero de copia, siempre
    # existe el id=0, porque debe haber un nodo de cada tipo
    # por lo menos

OUTPUT_COPIES=2
    # debe tener el mismo largo que OUTPUT_QUEUE_NAMES y
    # es una lista de numeros

# venvs
INPUT_EXCHANGE_NAMES=sanitize-credits,filter-release_date_since_2000
INPUT_QUEUE_NAMES=join-sanitize-id_id,join-filter-id_id
    # ambos tienen que tener la misma cantidad de campos
    # hay que appendear -%d a la hora de crear las colas y enviar mensajes

OUTPUT_EXCHANGE_NAME=join-id_id
OUTPUT_QUEUE_NAMES=explode-cast
    # hay que appendear -%d a la hora de crear las colas y enviar mensajes
OUTPUT_DELIVERY_TYPES=direct
    # este no tiene nada que ver con el tipo de exchange, esto es para saber
    # como mandar los mensajes a las copias, estos 2 ultimos campos tienen
    # que tener la misma cantidad de campos

LOG_LEVEL=DEBUG
SELECT=id,title,cast
LEFT_KEY=id
RIGHT_KEY=id
    # todo esto queda igual