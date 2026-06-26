# Supuestos y decisiones de negocio

Los datos solo tienen granularidad de **día** (no timestamp), lo que obliga a
fijar criterios explícitos. Cambiar cualquiera de estos es un cambio de una sola
línea en `config.py` (PySpark) o en los literales del `.sql`.

### 1. "Última semana" = últimos 7 días de prints
`serving_window_days = 7`. Con `max(day) = 2020-11-30`, la ventana servida es
**2020-11-24 → 2020-11-30** (113.336 prints). Se calcula con el máximo real del
dataset, no con una fecha fija, para que el pipeline sea reproducible.

### 2. "3 semanas previas" = ventana `[day-21, day-1]`
`feature_window_days = 21`. Para un print del día *D*, se cuentan eventos en los
**21 días estrictamente anteriores** a *D*. El día del propio print **no** entra
(a granularidad de día, "antes del print" significa días `< D`).

### 3. Un print está "clickeado" si existe un tap el mismo (día, usuario, value_prop)
Los taps no traen un `print_id`, así que el match es por
`(day, user_id, value_prop)`. La `position` no se usa en el match del label
(puede diferir) pero sí forma parte de la PK del print.

### 4. Deduplicación
* **prints / taps**: se deduplican por clave natural `(day, user_id, value_prop, position)`.
* **pays**: NO se deduplican — dos pagos idénticos el mismo día son eventos reales.
* El Gold además deduplica los prints base por PK de forma defensiva.

### 5. Eventos del mismo día que el print
Quedan **fuera** de la ventana `[day-21, day-1]`. Un pago realizado el mismo día
del print no cuenta en `amount_3w` (la ventana es estrictamente previa).

### 6. Manejo de nulos
Filas con `value_prop` o `user_id` nulos se descartan en Silver. Las features sin
historial quedan en `0` (no `NULL`) para ser directamente consumibles por modelos.
