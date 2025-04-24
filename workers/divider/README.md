# Divider Worker – Sistema Distribuido de Cálculo de Razones

Este módulo implementa un worker `Divider`, cuya funcionalidad consiste en **calcular la razón entre el ingreso (revenue) y el presupuesto (budget)** de un conjunto de registros, generando un nuevo campo con el valor calculado.

## ⚙️ Funcionalidad

El worker `Divider`:

- Recibe batches de datos codificados.
- Extrae los valores de los campos `revenue` y `budget` de cada registro.
- Calcula la razón `rate_revenue_budget` como `revenue / budget`.
- Si ambos valores son mayores que cero, agrega el campo `rate_revenue_budget` al registro con el valor calculado.
- Al final del flujo (`EOF`), publica un mensaje de cierre.

## 🔐 Configuración

La estructura de configuración (`DividerConfig`) debe definir:

- `Config`: Configuración general del worker base.
