# Gateway

Intermediario entre los clientes y los workers del sistema.

## 🚀 Funcionalidad

- **Protocolo de capa de transporte** a través de TCP.
- **Envío de archivos CSV** a través de lotes.
- **Envío de resultados de consultas** desde el servidor y almacenamiento de los resultados en archivos CSV.

## 🔐 Configuración

**Variables de entorno**:
El gateway obtiene la configuración desde las variables de entorno definidas en el archivo `config`.

Las variables esperadas son:

- `URL`: Url del nodo de rabbitmq.
- `HOST`: Host donde escuchar por clientes.
- `PORT`: Puerto de conección a clientes.
- `BACKLOG`: Tamaoñ del buffer de conexiones TCP esperando.
- `INPUT_EXCHANGE_NAMES`: Lista de nombres de exchanges entrantes.
- `INPUT_QUEUE_NAMES`: Lista de nombres de las colas entrantes.
- `OUTPUT_EXCHANGE_NAMES`: Lista de nombres de exchanges salientes.
- `OUTPUT_QUEUE_NAMES`: Lista de nombres de colas salientes.
- `HEALTH_CHECK_PORT`: Puerto en donde escuchar por keep alives.
- `KEEP_ALIVE_RETRIES`: Cantidad de veces a reintentar enviar respuesta al keep alive.
- `LOG_LEVEL`: Nivel de logueo del nodo.
- `ID`: id del nodo, para el gateway es siempre 0.
- `INPUT_COPIES`: Lista con la cantidad de replicas que tiene cada cola entrante.
- `OUTPUT_COPIES`: Lista con la cantidad de replicas que tiene cada cola saliente.
