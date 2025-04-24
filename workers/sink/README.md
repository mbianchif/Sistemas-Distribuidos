# Sink Worker – Sistema Distribuido de Análisis de Películas

Este módulo implementa un worker de tipo `Sink`, cuya función principal es **recibir los resultados procesados por otros workers** y publicarlos como salida final utilizando una query específica.

## ⚙️ Funcionalidad

El worker `Sink`:

- Recibe batches de datos procesados desde workers previos.
- Decodifica y publica estos datos hacia una cola o sistema externo asociado.
- Usa una `Query` configurada para categorizar la publicación de los resultados.
- También se encarga de publicar mensajes `EOF` (fin de flujo) asociados a la misma query.

## 🚀 Métodos principales

### `New(con *config.SinkConfig, log *logging.Logger) (*Sink, error)`
Crea e inicializa el worker `Sink`, conectándolo al sistema general con una configuración y logger específicos.

### `Run() error`
Ejecuta el ciclo de vida del worker, utilizando la lógica definida en su clase base.

### `Batch(data []byte) bool`
Procesa un batch de resultados y lo publica junto con una query. Si hay errores de decodificación, detiene la ejecución con logs detallados.

### `Eof(data []byte) bool`
Publica un mensaje `EOF` con la misma query configurada.

### `Error(data []byte) bool`
Registra un mensaje de error al recibir un paquete no válido o inesperado.

## 🔐 Configuración

La configuración de este worker (`SinkConfig`) debe incluir:

- `Query`: Identificador de la query a la cual los resultados pertenecen.
- `Config`: Parámetros generales para el worker base.
