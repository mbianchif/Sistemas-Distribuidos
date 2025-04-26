# MinMax Worker – Sistema Distribuido de Identificación de Mínimos y Máximos

Este módulo implementa un worker `MinMax`, cuya funcionalidad consiste en **identificar los elementos con el valor mínimo y máximo** de una clave numérica específica en un flujo de datos procesado en batches.

## ⚙️ Funcionalidad

El worker `MinMax`:

- Recibe batches de datos codificados.
- Extrae el valor asociado a una clave numérica (`Key`) especificada en la configuración.
- Compara y mantiene una referencia al elemento con el valor **mínimo** y al de **máximo** observado hasta el momento.
- Al finalizar el flujo (`EOF`), publica ambos elementos (mínimo y máximo) a través de la cola correspondiente.

## 🔐 Configuración

La estructura de configuración (`MinMaxConfig`) debe definir:

- `Key`: Clave sobre la cual se realiza la comparación (debe contener valores numéricos).
- `Config`: Configuración general del worker base.

## 🧠 Lógica de comparación

Durante el procesamiento de cada batch:
- Si el campo clave no está presente o su valor no puede convertirse a `float64`, el elemento es ignorado.
- Si es el primer batch recibido, inicializa los valores mínimo y máximo con el primer valor válido.
- Para cada nuevo elemento válido, compara su valor con los valores actuales y actualiza `min` y `max` si corresponde.
