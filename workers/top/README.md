# Top Worker – Sistema Distribuido de Análisis de Películas

Este módulo implementa un worker `Top`, cuya funcionalidad consiste en mantener los **N elementos más altos** (por una clave numérica) dentro de un flujo de datos procesado en batches.

## ⚙️ Funcionalidad

El worker `Top`:

- Recibe batches de datos codificados.
- Extrae y ordena los valores numéricos según una clave (`key`) especificada en la configuración.
- Mantiene únicamente los `N` valores más altos (definido por `Amount` en la configuración).
- Al final del flujo (`EOF`), publica los resultados a través de la cola correspondiente.

## 🚀 Métodos principales

### `New(con *config.TopConfig, log *logging.Logger) (*Top, error)`
Constructor del worker. Inicializa la configuración y estructuras internas.

### `Run() error`
Ejecuta el ciclo de vida del worker, delegando en la base.

### `Batch(data []byte) bool`
Procesa un batch de datos decodificado. Ordena y guarda los top-N elementos.

### `Eof(data []byte) bool`
Envía los resultados almacenados (top-N) al finalizar el procesamiento.

### `Error(data []byte) bool`
Maneja errores de tipo `ERROR`.

## 🔐 Configuración

La estructura de configuración (`TopConfig`) debe definir:

- `Key`: Clave sobre la cual se aplica el criterio de orden (valor numérico).
- `Amount`: Número máximo de elementos a mantener.
- `Config`: Configuración general del worker base.

