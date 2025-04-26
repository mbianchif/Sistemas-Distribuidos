# Sanitize Worker – Sistema Distribuido de Limpieza de Datos de Películas

Este módulo implementa un worker `Sanitize`, cuya funcionalidad consiste en **limpiar, transformar y validar registros** provenientes de archivos CSV para su posterior análisis en un sistema distribuido.

## ⚙️ Funcionalidad

El worker `Sanitize`:

- Lee datos en formato CSV recibidos como batches.
- Aplica un proceso de limpieza y transformación dependiendo del tipo de dataset (películas, créditos o ratings).
- Extrae los campos relevantes, remueve entradas inválidas y transforma campos complejos (como listas JSON) a strings planos.
- Publica los registros limpios como batches válidos.
- Publica una señal de finalización (`EOF`) al término del procesamiento.

## 🧠 Handlers disponibles

La lógica de transformación varía según el handler especificado en la configuración:

### `movies`
Extrae campos como:
- `id`, `title`, `release_date`, `overview`, `budget`, `revenue`
- `genres`, `production_countries`, `spoken_languages`

Transformaciones:
- Reemplazo de saltos de línea en `overview`.
- Extracción de listas (`genres`, etc.) desde campos con formato JSON-like.
- Validación de campos obligatorios y limpieza de espacios.

### `credits`
Extrae campos como:
- `id`, `cast`

Transformaciones:
- Extracción de nombres desde campos tipo JSON.

### `ratings`
Extrae campos como:
- `movieId`, `rating`, `timestamp`

Transformaciones:
- Conversión de timestamp en formato UNIX a fecha legible `YYYY-MM-DD HH:mm:ss`.

## 🔐 Configuración

La estructura de configuración (`SanitizeConfig`) debe definir:

- `Handler`: Tipo de dataset a sanitizar (`movies`, `credits`, `ratings`).
- `Config`: Configuración general del worker base.

## 🧩 Integración

Este worker está diseñado para ser el **primer paso en la cadena de procesamiento**, garantizando que los datos que lleguen a los siguientes módulos (como análisis de sentimiento, agregación, ranking, etc.) estén completos, bien formateados y limpios.
