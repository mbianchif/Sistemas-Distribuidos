# Sentiment Worker – Sistema Distribuido de Análisis de Películas

Este worker `Sentiment` realiza análisis de sentimientos sobre datos textuales, particularmente sobre el campo `"overview"` de cada mensaje recibido. Utiliza un modelo preentrenado de lenguaje natural para clasificar el contenido como **positivo** o **negativo**.

## ⚙️ Funcionalidad

El worker `Sentiment`:

- Recibe batches de datos que contienen una descripción textual (`overview`).
- Utiliza un modelo de aprendizaje automático para predecir el **sentimiento** del texto.
- Agrega un campo `"sentiment"` al mensaje original con el valor `"positive"` o `"negative"`.
- Publica los resultados procesados.
- Maneja el mensaje de fin de flujo (`EOF`) y mensajes de error.

## 🚀 Métodos principales

### `New(con *config.SentimentConfig, log *logging.Logger) (*Sentiment, error)`
Inicializa el worker y carga el modelo de análisis de sentimientos preentrenado.

### `Run() error`
Ejecuta el ciclo de vida del worker, delegando en la lógica base.

### `Batch(data []byte) bool`
Decodifica los datos entrantes y procesa cada mensaje aplicando análisis de sentimiento al campo `"overview"`.

### `Eof(data []byte) bool`
Publica el mensaje de fin de flujo (`EOF`) cuando se recibe.

### `Error(data []byte) bool`
Registra un error al recibir un mensaje inesperado o mal formado.

## 🧠 Análisis de Sentimiento

Se utiliza la librería [`cdipaolo/sentiment`](https://github.com/cdipaolo/sentiment), que provee un modelo eficiente para clasificar frases como positivas o negativas.
