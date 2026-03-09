import logging
import time
from datetime import date, datetime, timedelta
from pathlib import Path
from typing import Any

import psycopg2
try:
    from hdbcli import dbapi
except ImportError:
    dbapi = None

from sap_report.domain import fecha_a_cuid
from sap_report.infrastructure.config import Settings


LOGGER = logging.getLogger(__name__)
# Rutas SQL por motor.
SAP_QUERY_PATH = Path(__file__).resolve().parent / "queries" / "SAP.sql"
SAP_NC_QUERY_PATH = Path(__file__).resolve().parent / "queries" / "sap_nc.sql"
PG_QUERY_PATH = Path(__file__).resolve().parent / "queries" / "TUTATI.sql"
PG_NC_QUERY_PATH = Path(__file__).resolve().parent / "queries" / "tutati_nc.sql"


class SapHanaRepository:
    def __init__(self, settings: Settings) -> None:
        # Valida dependencia nativa de SAP HANA.
        if dbapi is None:
            raise RuntimeError("Falta dependencia hdbcli. Instala con: pip install hdbcli")
        self._settings = settings
        # Carga plantilla SQL de SAP.
        self._query_template = SAP_QUERY_PATH.read_text(encoding="utf-8")
        self._query_nc_template = SAP_NC_QUERY_PATH.read_text(encoding="utf-8")

    def ejecutar_consulta_sql(
        self,
        fecha_inicio: date,
        fecha_fin: date,
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        # Reemplaza fechas en formato esperado por SAP.
        sql = self._render_query(fecha_inicio, fecha_fin)
        return self._ejecutar_sql(sql)

    def ejecutar_consulta_nc_sql(
        self,
        fecha_inicio: date,
        fecha_fin: date,
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        # Ejecuta SQL de notas de credito en SAP.
        sql = self._render_query(fecha_inicio, fecha_fin, self._query_nc_template)
        return self._ejecutar_sql(sql)

    def _ejecutar_sql(self, sql: str) -> tuple[list[tuple[Any, ...]], list[str]]:
        # Reintenta la conexion ante fallos temporales.
        for intento in range(1, self._settings.reintentos + 1):
            conn = None
            cur = None
            try:
                conn_kwargs = {
                    # Config minima equivalente a DBeaver.
                    "address": self._settings.sap_hana_host,
                    "port": self._settings.sap_hana_port,
                    "user": self._settings.sap_hana_user,
                    "password": self._settings.sap_hana_password,
                }
                if self._settings.sap_hana_encrypt is not None:
                    # Opcional: se activa solo si viene en .env.
                    conn_kwargs["encrypt"] = self._settings.sap_hana_encrypt
                if self._settings.sap_hana_ssl_validate_certificate is not None:
                    conn_kwargs["sslValidateCertificate"] = self._settings.sap_hana_ssl_validate_certificate
                if self._settings.sap_hana_connect_timeout is not None:
                    conn_kwargs["connecttimeout"] = self._settings.sap_hana_connect_timeout
                if self._settings.sap_hana_ssl_trust_store:
                    conn_kwargs["sslTrustStore"] = self._settings.sap_hana_ssl_trust_store
                if self._settings.sap_hana_ssl_key_store_password:
                    conn_kwargs["sslKeyStorePassword"] = self._settings.sap_hana_ssl_key_store_password

                conn = dbapi.connect(
                    **conn_kwargs,
                )
                cur = conn.cursor()
                # Ejecuta SQL y devuelve filas + cabeceras.
                cur.execute(sql)
                rows = cur.fetchall()
                if cur.description is None:
                    raise RuntimeError("La consulta SAP no devolvio metadatos de columnas.")
                cols = [c[0] for c in cur.description]
                return rows, cols
            except dbapi.Error as exc:
                # Registra diagnostico detallado por intento.
                LOGGER.warning(
                    "Conexion SAP caida (intento %s/%s): %s | encrypt=%s | sslValidateCertificate=%s",
                    intento,
                    self._settings.reintentos,
                    exc,
                    self._settings.sap_hana_encrypt,
                    self._settings.sap_hana_ssl_validate_certificate,
                )
                if intento == self._settings.reintentos:
                    raise
                time.sleep(self._settings.espera_segundos)
            finally:
                if cur:
                    cur.close()
                if conn:
                    conn.close()

        raise RuntimeError("No se pudo ejecutar la consulta SAP tras todos los reintentos.")

    def probar_conexion(self) -> None:
        # Conexion corta para validar credenciales/red sin ejecutar reporte.
        conn_kwargs = {
            "address": self._settings.sap_hana_host,
            "port": self._settings.sap_hana_port,
            "user": self._settings.sap_hana_user,
            "password": self._settings.sap_hana_password,
        }
        if self._settings.sap_hana_encrypt is not None:
            conn_kwargs["encrypt"] = self._settings.sap_hana_encrypt
        if self._settings.sap_hana_ssl_validate_certificate is not None:
            conn_kwargs["sslValidateCertificate"] = self._settings.sap_hana_ssl_validate_certificate
        if self._settings.sap_hana_connect_timeout is not None:
            conn_kwargs["connecttimeout"] = self._settings.sap_hana_connect_timeout
        if self._settings.sap_hana_ssl_trust_store:
            conn_kwargs["sslTrustStore"] = self._settings.sap_hana_ssl_trust_store
        if self._settings.sap_hana_ssl_key_store_password:
            conn_kwargs["sslKeyStorePassword"] = self._settings.sap_hana_ssl_key_store_password

        conn = dbapi.connect(**conn_kwargs)
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1 FROM DUMMY")
            cur.fetchone()
        finally:
            cur.close()
            conn.close()

    def _render_query(self, fecha_inicio: date, fecha_fin: date, query_template: str | None = None) -> str:
        # Formato de fecha solicitado: YYYY-MM-DD.
        template = query_template or self._query_template
        return (
            template
            .replace("{{fecha_inicio}}", fecha_inicio.strftime("%Y-%m-%d"))
            .replace("{{fecha_fin}}", fecha_fin.strftime("%Y-%m-%d"))
        )


class PostgresRepository:
    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        # Carga SQL parametrizado para PostgreSQL.
        self._query = PG_QUERY_PATH.read_text(encoding="utf-8")
        self._query_nc = PG_NC_QUERY_PATH.read_text(encoding="utf-8")

    def ejecutar_consulta_sql(
        self,
        fecha_inicio: date,
        fecha_fin: date,
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        return self._ejecutar_sql(self._query, fecha_inicio, fecha_fin)

    def ejecutar_consulta_nc_sql(
        self,
        fecha_inicio: date,
        fecha_fin: date,
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        # Ejecuta SQL de notas de credito en PostgreSQL.
        return self._ejecutar_sql(self._query_nc, fecha_inicio, fecha_fin)

    def _ejecutar_sql(
        self,
        query: str,
        fecha_inicio: date,
        fecha_fin: date,
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        # Convierte rango fecha->CUID para filtro en PostgreSQL.
        inicio_dt = datetime.combine(fecha_inicio, datetime.min.time())
        fin_dt = datetime.combine(fecha_fin + timedelta(days=1), datetime.min.time())
        params = {
            "fecha1": fecha_a_cuid(inicio_dt),
            "fecha2": fecha_a_cuid(fin_dt),
        }

        # Reintenta la conexion ante cortes.
        for intento in range(1, self._settings.reintentos + 1):
            conn = None
            cur = None
            try:
                conn = psycopg2.connect(
                    host=self._settings.pg_host,
                    dbname=self._settings.pg_name,
                    user=self._settings.pg_user,
                    password=self._settings.pg_password,
                    port=self._settings.pg_port,
                    sslmode=self._settings.pg_sslmode,
                    connect_timeout=self._settings.pg_connect_timeout,
                    keepalives=1,
                    keepalives_idle=30,
                    keepalives_interval=10,
                    keepalives_count=5,
                )
                conn.autocommit = True
                cur = conn.cursor()
                # Ejecuta SQL y devuelve filas + cabeceras.
                cur.execute(query, params)
                rows = cur.fetchall()
                if cur.description is None:
                    raise RuntimeError("La consulta PostgreSQL no devolvio metadatos de columnas.")
                cols = [c[0] for c in cur.description]
                return rows, cols
            except psycopg2.OperationalError as exc:
                # Log por intento para diagnostico de red.
                LOGGER.warning(
                    "Conexion PostgreSQL caida (intento %s/%s): %s",
                    intento,
                    self._settings.reintentos,
                    exc,
                )
                if intento == self._settings.reintentos:
                    raise
                time.sleep(self._settings.espera_segundos)
            finally:
                if cur:
                    cur.close()
                if conn:
                    conn.close()

        raise RuntimeError("No se pudo ejecutar la consulta PostgreSQL tras todos los reintentos.")

    def probar_conexion(self) -> None:
        # Conexion corta para validar acceso a PostgreSQL.
        conn = psycopg2.connect(
            host=self._settings.pg_host,
            dbname=self._settings.pg_name,
            user=self._settings.pg_user,
            password=self._settings.pg_password,
            port=self._settings.pg_port,
            sslmode=self._settings.pg_sslmode,
            connect_timeout=self._settings.pg_connect_timeout,
        )
        cur = conn.cursor()
        try:
            cur.execute("SELECT 1")
            cur.fetchone()
        finally:
            cur.close()
            conn.close()
