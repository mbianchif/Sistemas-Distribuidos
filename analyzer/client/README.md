# Cliente CSV para Transferencia a Gateway

Este cliente permite conectar a un servidor gateway y transferir archivos CSV en lotes, recibir los resultados de las consultas y almacenarlos en un directorio especificado.

## 🚀 Funcionalidad

- **Conexión a un servidor gateway** a través de TCP.
- **Envío de archivos CSV** a través de lotes.
- **Recepción de resultados de consultas** desde el servidor y almacenamiento de los resultados en archivos CSV.

## 🔐 Configuración

1. **Variables de entorno**:
   El cliente obtiene la configuración desde las variables de entorno definidas en el archivo `config`.

   Las variables esperadas son:

   - `GATEWAY_HOST`: La dirección IP o nombre del host del servidor gateway.
   - `GATEWAY_PORT`: El puerto en el que el servidor gateway escucha.
   - `LOG_LEVEL`: Nivel de logs, puede ser `DEBUG`, `INFO`, `ERROR`, etc.
   - `STORAGE`: El directorio donde almacenar los resultados de las consultas.

2. **Archivos CSV**:
   Los archivos CSV a enviar están definidos en el array `files`, en este caso, los archivos son:
   - `movies.csv`
   - `credits.csv`
   - `ratings.csv`
