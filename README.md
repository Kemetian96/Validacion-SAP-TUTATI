# SAP Report

Aplicacion de escritorio para ejecutar consultas a PostgreSQL por rango de fechas y exportar a Excel.

## Ejecutar

```powershell
pip install -r requirements.txt
python app.py
```

## Configuracion

Variables en `.env` (usa `.env.example` como base):

- `PG_HOST`
- `PG_NAME`
- `PG_USER`
- `PG_PASSWORD`
- `PG_PORT`
- `PG_SSLMODE`
- `PG_CONNECT_TIMEOUT`
- `SAP_HANA_HOST`
- `SAP_HANA_PORT`
- `SAP_HANA_USER`
- `SAP_HANA_PASSWORD`
- `SAP_HANA_ENCRYPT`
- `SAP_HANA_SSL_VALIDATE_CERTIFICATE`
- `SAP_HANA_SSL_TRUST_STORE`
- `SAP_HANA_SSL_KEY_STORE_PASSWORD`
- `SAP_HANA_CONNECT_TIMEOUT`
- `SAP_OUTPUT_PATH`
- `PG_OUTPUT_PATH`
- `COMPARACION_OUTPUT_PATH`
- `REINTENTOS_CONEXION`
- `ESPERA_REINTENTO_SEGUNDOS`
- `UI_WIDTH`
- `UI_HEIGHT`
- `FECHA_INICIO`
- `FECHA_FIN`

## Estructura

- `src/sap_report/ui`: interfaz Tkinter.
- `src/sap_report/application`: casos de uso.
- `src/sap_report/domain`: reglas de dominio.
- `src/sap_report/infrastructure`: config, DB y export.
