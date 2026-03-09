# SAP Report

Aplicacion de escritorio (Tkinter) para ejecutar reportes por rango de fechas contra dos fuentes:

- SAP HANA
- PostgreSQL

Genera tres archivos Excel:

- `OUTPUT/SAP.xlsx`
- `OUTPUT/TUTATI.xlsx`
- `OUTPUT/COMPARACION.xlsx`

## Funcionalidades

- Selector de fechas en UI (`yyyy-mm-dd`).
- Boton para ejecutar reporte completo.
- Boton para probar conexiones (SAP y PostgreSQL).
- Ejecucion por lotes diarios para evitar consultas muy pesadas.
- Reintentos de conexion configurables.
- Exportacion de reportes base y acumulados de NC.
- Comparacion entre SAP y TUTATI por identificador.
- Deteccion de faltantes por cantidad de ocurrencias.
- Deteccion de diferencias de monto con umbral (`|SUMA - TOTAL| > 0.1`).

## Requisitos

- Python `>= 3.11`
- Dependencias en `requirements.txt`

Instalacion:

```powershell
pip install -r requirements.txt
```

Ejecucion:

```powershell
python app.py
```

## Configuracion (`.env`)

Usa `.env.example` como base.

### PostgreSQL

- `PG_HOST` (o alias legacy: `DB_HOST`)
- `PG_NAME` (o alias legacy: `DB_NAME`, default `main`)
- `PG_USER` (o alias legacy: `DB_USER`)
- `PG_PASSWORD` (o alias legacy: `DB_PASSWORD`)
- `PG_PORT` (o alias legacy: `DB_PORT`, default `5432`)
- `PG_SSLMODE` (o alias legacy: `DB_SSLMODE`, default `require`)
- `PG_CONNECT_TIMEOUT` (o alias legacy: `DB_CONNECT_TIMEOUT`, default `10`)

### SAP HANA

- `SAP_HANA_HOST` (default `172.31.28.162`)
- `SAP_HANA_PORT` (default `30015`)
- `SAP_HANA_USER` (obligatoria)
- `SAP_HANA_PASSWORD` (obligatoria)
- `SAP_HANA_ENCRYPT` (`true/false`, opcional)
- `SAP_HANA_SSL_VALIDATE_CERTIFICATE` (`true/false`, opcional)
- `SAP_HANA_SSL_TRUST_STORE` (opcional)
- `SAP_HANA_SSL_KEY_STORE_PASSWORD` (opcional)
- `SAP_HANA_CONNECT_TIMEOUT` (opcional)

### Salidas y ejecucion

- `SAP_OUTPUT_PATH` (default `OUTPUT/SAP.xlsx`)
- `PG_OUTPUT_PATH` (default `OUTPUT/TUTATI.xlsx`)
- `COMPARACION_OUTPUT_PATH` (default `OUTPUT/COMPARACION.xlsx`)
- `REINTENTOS_CONEXION` (default `5`)
- `ESPERA_REINTENTO_SEGUNDOS` (default `10`)
- `FECHA_INICIO` (default `2026-01-01`)
- `FECHA_FIN` (default `2026-01-01`)

### UI

- `UI_WIDTH` (default `360`)
- `UI_HEIGHT` (default `260`)

## Consultas SQL

Rutas:

- `src/sap_report/infrastructure/db/queries/SAP.sql`
- `src/sap_report/infrastructure/db/queries/sap_nc.sql`
- `src/sap_report/infrastructure/db/queries/TUTATI.sql`
- `src/sap_report/infrastructure/db/queries/tutati_nc.sql`

Reglas de parametros:

- SAP usa reemplazo de plantilla: `{{fecha_inicio}}` y `{{fecha_fin}}` en formato `YYYY-MM-DD`.
- PostgreSQL usa parametros `%(fecha1)s` y `%(fecha2)s` (rango CUID).

## Salidas Excel

### `SAP.xlsx`

- Hoja `Reporte`: resultado de `SAP.sql`.
- Hoja `Acumulado_NC`: consolidado por referencia de `sap_nc.sql`.

### `TUTATI.xlsx`

- Hoja `Reporte`: resultado de `TUTATI.sql`.
- Hoja `Acumulado_NC`: consolidado por EID de `tutati_nc.sql`.

### `COMPARACION.xlsx`

- Hoja `Comparacion`: comparacion del reporte base.
- Hoja `Comparacion_NC`: comparacion del reporte NC.

Cada hoja de comparacion contiene 3 bloques en una misma pestaña, separados por 1 columna:

- `RESUMEN`
- `FALTANTES`
- `DIFERENCIAS`

Columnas actuales del bloque `DIFERENCIAS`:

- `u_bot_docentry`
- `uid_orders`
- `fecha`
- `suma_sap`
- `total_tutati`
- `diferencia`

## Estructura del proyecto

- `app.py`: launcher.
- `src/sap_report/main.py`: composicion de dependencias e inicio UI.
- `src/sap_report/ui/`: interfaz Tkinter.
- `src/sap_report/application/`: logica de caso de uso (`ReportService`).
- `src/sap_report/domain/`: utilidades de dominio (ej. conversion a CUID).
- `src/sap_report/infrastructure/config.py`: carga y validacion de entorno.
- `src/sap_report/infrastructure/db/repository.py`: acceso a SAP HANA y PostgreSQL.
- `src/sap_report/infrastructure/export/excel_writer.py`: escritura Excel.
- `tests/unit/`: pruebas unitarias.

## Notas tecnicas

- Si en una ruta de salida configuras `.xls`, la app exporta automaticamente a `.xlsx`.
- Si falla una fuente, se registra el error y se intenta continuar con la otra.
- Si fallan ambas fuentes, la ejecucion termina con error global.
