# GroupBy Worker

Este módulo implementa un worker `GroupBy`, cuya funcionalidad consiste en **agrupar registros** dentro de un flujo de datos, según claves y operaciones de agregación definidas dinámicamente en la configuración.

## ⚙️ Funcionalidad

El worker `GroupBy`:

- Recibe batches de datos codificados.
- Agrupa los registros basándose en una o más claves (`GROUP_KEY`) configuradas.
- Aplica funciones de agregación sobre campos específicos (`AGGREGATOR_KEY`).
- Emite batches de resultados agrupados una vez procesados todos los datos.
- Al final del flujo (`EOF`), publica un mensaje de cierre.

## 🔐 Configuración

La estructura de configuración (`GroupByConfig`) debe definir:

- `GROUP_KEY`: Lista de claves por las cuales se realiza el agrupamiento.
- `AGGREGATOR`: Operación a aplicar sobre campos numéricos.
- `AGGREGATOR_KEY`: Columna a la que se le aplica la agregación.
- `STORAGE`: Nombre de la columna donde se almacenará el resultado de la agregación.

## 🧠 Tipos de agregación

### 📊 `count`
Cuenta la cantidad de registros en cada grupo.

**Ejemplo:**
- Campo: `any`
- Resultado: Número de registros agrupados.

---

### 📊 `sum`
Suma los valores de un campo numérico en cada grupo.

**Ejemplo:**
- Campo: `rating`
- Resultado: Suma total de ratings del grupo.

---

### 📊 `mean`
Calcula el promedio de los valores de un campo numérico en cada grupo.

**Ejemplo:**
- Campo: `rating`
- Resultado: Promedio de ratings del grupo.

