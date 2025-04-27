# Sentiment Worker

Este worker `Sentiment` realiza análisis de sentimientos sobre datos textuales, particularmente sobre el campo `"overview"` de cada mensaje recibido. Utiliza un modelo preentrenado de lenguaje natural para clasificar el contenido como **positivo** o **negativo**.

## ⚙️ Funcionalidad

El worker `Sentiment`:

- Recibe batches de datos que contienen una descripción textual (`overview`).
- Utiliza un modelo de aprendizaje automático para predecir el **sentimiento** del texto.
- Agrega un campo `"sentiment"` al mensaje original con el valor `"positive"` o `"negative"`.
- Publica los resultados procesados.
- Maneja el mensaje de fin de flujo (`EOF`) y mensajes de error.


## 🧠 Análisis de Sentimiento

Se utiliza la librería [`cdipaolo/sentiment`](https://github.com/cdipaolo/sentiment), que provee un modelo eficiente para clasificar frases como positivas o negativas.
