# Filter Worker – Sistema Distribuido de Filtrado de Datos

Este módulo implementa un worker `Filter`, cuya funcionalidad consiste en **filtrar registros** dentro de un flujo de datos, según condiciones definidas dinámicamente en la configuración.

## ⚙️ Funcionalidad

El worker `Filter`:

- Recibe batches de datos codificados.
- Aplica un filtro específico a cada registro, según una clave (`Key`) y un valor (`Value`) configurados.
- Si un registro cumple con la condición, se incluye en el batch de salida.
- En caso contrario, se descarta.
- Al final del flujo (`EOF`), publica un mensaje de cierre.

## 🔐 Configuración

La estructura de configuración (`FilterConfig`) debe definir:

- `Key`: Clave sobre la cual se aplica el filtro.
- `Value`: Valor de referencia usado para evaluar la condición.
- `Handler`: Tipo de filtro a aplicar. Puede ser:
  - `"range"`: Evalúa si el año (extraído de una fecha) está dentro de un rango solo inclusivo a la izquierda.
  - `"contains"`: Verifica si el campo contiene todos los valores especificados.
  - `"length"`: Verifica si el campo tiene una cantidad determinada de elementos.
- `Config`: Configuración general del worker base.

## 🧠 Tipos de filtro

### 🔍 `range`
Filtra registros cuya clave contiene una fecha en formato `"YYYY-MM-DD"` y se desea incluir solo si el año está dentro de un rango definido.

**Ejemplo:**
- Clave: `release_date`
- Valor: `"2000..2010"`
- Acepta: `"2005-06-01"`
- Rechaza: `"1999-12-31"`

---

### 🔍 `contains`
Filtra registros si **contienen todos los valores** definidos, separados por coma.

**Ejemplo:**
- Clave: `genres`
- Valor: `"Action,Drama"`
- Acepta: `"Action,Drama,Thriller"`
- Rechaza: `"Action,Comedy"`

---

### 🔍 `length`
Filtra registros cuya lista de valores (separados por coma) tiene una longitud específica.

**Ejemplo:**
- Clave: `actors`
- Valor: `"3"`
- Acepta: `"Actor A,Actor B,Actor C"`
- Rechaza: `"Actor A,Actor B"`
