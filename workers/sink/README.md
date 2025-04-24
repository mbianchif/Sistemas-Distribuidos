# Sink Worker – Sistema Distribuido de Análisis de Películas

Este módulo implementa un worker de tipo `Sink`, cuya función principal es **recibir los resultados procesados por otros workers** y publicarlos como salida final utilizando una query específica.

## ⚙️ Funcionalidad

El worker `Sink`:

- Recibe batches de datos procesados desde workers previos.
- Decodifica y publica estos datos hacia una cola o sistema externo asociado.
- Usa una `Query` configurada para categorizar la publicación de los resultados.
- También se encarga de publicar mensajes `EOF` (fin de flujo) asociados a la misma query.

## 🔐 Configuración

La configuración de este worker (`SinkConfig`) debe incluir:

- `Query`: Identificador de la query a la cual los resultados pertenecen.
- `Config`: Parámetros generales para el worker base.
