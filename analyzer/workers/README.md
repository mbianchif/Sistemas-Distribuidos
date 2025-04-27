# Worker – Sistema Distribuido 

Este módulo implementa la base de los workers que implementa la comunicación con rabbit.

## 🔐 Configuración

La estructura de configuración (`Worker`) debe definir:

- `RABBIT_URL`: Url al proceso que corre el servicio de rabbitmq.
- `LOG_LEVEL`: Nivel de logeo.
- `INPUT_EXCHANGE_NAMES`: Lista de nombres de los exchanges de input.
- `INPUT_QUEUE_NAMES`: Lista de nombres de las colas para cada exchange (1:1).
- `OUTPUT_EXCHANGE_NAME`: Nombre del exchange de output.
- `OUTPUT_QUEUE_NAMES`: Lista de nombres de las colas de output.
- `OUTPUT_DELIVERY_TYPES`: Lista de tipo de delivery por cada cola.
    - `robin`: Despachará los mensajes en estilo _round-robin_ entre las réplicas.
    - `shard:{key}`: Despachará los mensajes en estilo _shard_ utilizando la clave proveída.
- `SELECT`: Lista de nombres de columnas que sobreviviran al procesado.
