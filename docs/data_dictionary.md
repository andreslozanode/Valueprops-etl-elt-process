# Diccionario de datos

> Perfilado real de los archivos fuente (noviembre 2020).

## Fuentes

### `prints.json` — impresiones de value props (JSONL)
508.616 líneas · 30 días (2020-11-01 → 2020-11-30) · 87.864 usuarios.

| Campo                   | Tipo   | Ejemplo              |
|-------------------------|--------|----------------------|
| `day`                   | string | `"2020-11-01"`       |
| `event_data.position`   | int    | `0` … `3`            |
| `event_data.value_prop` | string | `"cellphone_recharge"` |
| `user_id`               | long   | `98702`              |

### `taps.json` — clicks sobre value props (JSONL)
50.858 líneas · mismo esquema que `prints.json`.

### `pays.csv` — pagos (CSV con header)
756.484 filas · `total` ∈ [0.00, 199.95], media 50.25.

| Campo        | Tipo   | Ejemplo              |
|--------------|--------|----------------------|
| `pay_date`   | string | `"2020-11-01"`       |
| `total`      | double | `37.36`              |
| `user_id`    | long   | `79066`              |
| `value_prop` | string | `"cellphone_recharge"` |

**`value_prop` (7 categorías, consistentes en las 3 fuentes):**
`cellphone_recharge`, `credits_consumer`, `link_cobro`, `point`, `prepaid`,
`send_money`, `transport`.

## Salida: `gold.value_prop_features`
113.336 filas (prints de la última semana: 2020-11-24 → 2020-11-30).

| Columna         | Tipo      | Descripción                                                       |
|-----------------|-----------|-------------------------------------------------------------------|
| `day`           | date      | Día del print servido (PK)                                        |
| `user_id`       | bigint    | Usuario (PK)                                                      |
| `value_prop`    | string    | Value prop del print (PK)                                         |
| `position`      | int       | Posición en el carrusel (PK)                                      |
| `clicked`       | int       | **Label**: 1 si el usuario tapeó ese print, 0 si no              |
| `views_3w`      | bigint    | # veces que vio ese value_prop en [day-21, day-1]                |
| `taps_3w`       | bigint    | # veces que tapeó ese value_prop en [day-21, day-1]              |
| `pays_3w`       | bigint    | # pagos de ese value_prop en [day-21, day-1]                     |
| `amount_3w`     | double    | Monto acumulado pagado de ese value_prop en [day-21, day-1]      |
| `ctr_3w`        | double    | Feature derivada: `taps_3w / views_3w`                           |
| `avg_ticket_3w` | double    | Feature derivada: `amount_3w / pays_3w`                          |
| `_computed_at`  | timestamp | Marca de tiempo del cómputo                                       |

**Estadísticas de la salida** (validadas): click rate 10.03 %, `avg(views_3w)` 0.51,
`avg(taps_3w)` 0.05, `avg(pays_3w)` 0.76, `avg(amount_3w)` 37.77, `max(views_3w)` 5.
