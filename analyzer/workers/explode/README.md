# Explode Worker – Sistema Distribuido de Expansión de Campos

Este módulo implementa un worker `Explode`, cuya funcionalidad consiste en **expandir campos que contienen múltiples valores** en varios registros individuales.

## ⚙️ Funcionalidad

El worker `Explode`:

- Recibe batches de datos codificados.
- Identifica un campo específico (`Key`) que contiene una lista de elementos separados por comas.
- Genera un nuevo registro por cada valor en la lista, copiando los demás campos y reemplazando/renombrando el campo original con un nuevo nombre (`Rename`).
- Al final del flujo (`EOF`), publica un mensaje de cierre.

## 🔐 Configuración

La estructura de configuración (`ExplodeConfig`) debe definir:

- `Key`: Clave que contiene múltiples valores separados por comas.
- `Rename`: Nombre del nuevo campo en el que se colocará cada uno de los valores individuales.
- `Config`: Configuración general del worker base.
