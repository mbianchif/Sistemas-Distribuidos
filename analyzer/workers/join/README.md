# Join Worker

Este módulo implementa un worker `Join`, cuya funcionalidad consiste en **realizar una operación de join** entre elementos provenientes de diferentes fuentes (identificados por un `id` común) dentro de un flujo de datos procesado en batches.

## ⚙️ Funcionalidad

El worker `Join`:

- Recibe baches de datos codificados que contienen registros que pueden ser comparados por un par de claves `LEFT_KEY` y `RIGHT_KEY`.
- Agrupa internamente los registros por esas clave.
- Al recibir todos los datos necesarios de los distintos orígenes, une los campos y los publica como un único registro.
- La unión se realiza **solo si están presentes todos los campos requeridos**.

## 🔐 Configuración

La estructura de configuración (`JoinConfig`) debe definir:

- `LEFT_KEY`: Clave por la cual se agrupa un registro de la primer tabla.
- `RIGHT_KEY`: Clave por la cual se agrupa registro de la segunda tabla.
- `SHARDS`: Cantidad de workers y archivos de shardeo creados.

## 🧠 Lógica de unión

Durante el procesamiento de batches:

- Cada registro se guarda en una estructura de agrupación según el valor de los registros `LEFT_KEY` y `RIGHT_KEY`.
- El sistema verifica si para una clave dada ya se han recibido registros de **todas las fuentes necesarias**.
- Una vez completo, se realiza la unión de campos y se publica el nuevo registro.
