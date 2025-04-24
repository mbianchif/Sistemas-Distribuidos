# Join Worker – Sistema Distribuido de Unión de Datos

Este módulo implementa un worker `Join`, cuya funcionalidad consiste en **realizar una operación de join** entre elementos provenientes de diferentes fuentes (identificados por un `id` común) dentro de un flujo de datos procesado en batches.

## ⚙️ Funcionalidad

El worker `Join`:

- Recibe batches de datos codificados que contienen registros con una clave común (`JoinKey`).
- Agrupa internamente los registros por esa clave.
- Al recibir todos los datos necesarios de los distintos orígenes, une los campos y los publica como un único registro.
- La unión se realiza **solo si están presentes todos los campos requeridos**.

## 🔐 Configuración

La estructura de configuración (`JoinConfig`) debe definir:

- `JoinKey`: Clave por la cual se agruparán los registros.
- `Sources`: Lista de nombres de fuentes que se espera unir (por ejemplo, `["credits", "ratings"]`).
- `Config`: Configuración general del worker base.

## 🧠 Lógica de unión

Durante el procesamiento de batches:

- Cada registro se guarda en una estructura de agrupación según el valor de `JoinKey`.
- El sistema verifica si para una clave dada ya se han recibido registros de **todas las fuentes necesarias**.
- Una vez completo, se realiza la unión de campos y se publica el nuevo registro.
- Al finalizar (`EOF`), se pueden publicar las uniones parciales restantes si la configuración así lo permite.
