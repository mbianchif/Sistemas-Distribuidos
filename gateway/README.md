# Gateway – Servidor de Recepción y Procesamiento de Archivos CSV

Este módulo implementa un servidor que maneja la recepción de archivos CSV en lotes a través de un socket TCP, procesando los datos y enviándolos a través de una cola de mensajería (RabbitMQ). El servidor gestiona mensajes de tipo **batch**, **EOF** (fin de archivo) y **error**.

## ⚙️ Funcionalidad

El servidor CSV Transfer:

- Acepta conexiones entrantes de clientes a través de TCP.
- Recibe archivos CSV en formato de lotes (batches).
- Procesa los datos CSV en los lotes recibidos.
- De manera simultanea publica los resultados a través de una cola de mensajería (RabbitMQ).
- Gestiona el cierre de archivos (EOF) y errores durante la recepción.

## 🔐 Configuración

La estructura de configuración (`Config`) debe definir:

- `host`: Dirección IP o nombre de host del servidor donde escuchará las conexiones.
- `port`: Puerto en el que el servidor escuchará las conexiones entrantes.
- `backlog`: Número máximo de conexiones pendientes en la cola de aceptación.
- `rabbitmq_host`: Dirección del servidor RabbitMQ.
- `rabbitmq_port`: Puerto del servidor RabbitMQ.
- `queue_name`: Nombre de la cola en RabbitMQ donde se publicarán los mensajes procesados.
