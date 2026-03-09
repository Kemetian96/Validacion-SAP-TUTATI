import logging
from datetime import date, timedelta
from decimal import Decimal
from pathlib import Path
from typing import Any

from sap_report.domain import cuid_a_fecha
from sap_report.infrastructure.db import PostgresRepository, SapHanaRepository
from sap_report.infrastructure.export import (
    exportar_comparacion,
    exportar_excel,
    exportar_pestana_excel,
)


LOGGER = logging.getLogger(__name__)


class ReportService:
    def __init__(
        self,
        sap_repository: SapHanaRepository,
        postgres_repository: PostgresRepository,
        sap_output_path: Path,
        postgres_output_path: Path,
        comparacion_output_path: Path,
    ) -> None:
        # Dependencias de acceso a datos y rutas de salida.
        self._sap_repository = sap_repository
        self._postgres_repository = postgres_repository
        self._sap_output_path = sap_output_path
        self._postgres_output_path = postgres_output_path
        self._comparacion_output_path = comparacion_output_path

    def ejecutar_reporte(
        self,
        fecha_inicio_date: date,
        fecha_fin_date: date,
        status_cb=None,
    ) -> dict[str, int | str | None]:
        # Validacion basica del rango.
        if fecha_inicio_date > fecha_fin_date:
            raise ValueError("La fecha inicio no puede ser mayor a la fecha fin.")

        if status_cb:
            status_cb(f"Procesando rango: {fecha_inicio_date} -> {fecha_fin_date}")

        # Acumuladores y errores por fuente.
        sap_rows: list[tuple[Any, ...]] = []
        sap_cols: list[str] | None = None
        sap_nc_rows: list[tuple[Any, ...]] = []
        sap_nc_cols: list[str] | None = None
        pg_rows: list[tuple[Any, ...]] = []
        pg_cols: list[str] | None = None
        pg_nc_rows: list[tuple[Any, ...]] = []
        pg_nc_cols: list[str] | None = None
        sap_error: str | None = None
        pg_error: str | None = None

        try:
            # Ejecuta lotes SAP y exporta resultado.
            sap_rows, sap_cols = self._ejecutar_por_lotes(
                fecha_inicio_date,
                fecha_fin_date,
                self._sap_repository,
                "SAP",
                status_cb,
            )
            exportar_excel(sap_rows, sap_cols, self._sap_output_path)
            # Agrega pestaña de acumulado NC en SAP.xlsx.
            sap_nc_rows, sap_nc_cols = self._ejecutar_por_lotes(
                fecha_inicio_date,
                fecha_fin_date,
                self._sap_repository,
                "SAP_NC",
                status_cb,
                query_method_name="ejecutar_consulta_nc_sql",
            )
            sap_nc_acum_rows, sap_nc_acum_cols = _acumular_sap_nc(sap_nc_rows, sap_nc_cols)
            exportar_pestana_excel(
                sap_nc_acum_rows,
                sap_nc_acum_cols,
                self._sap_output_path,
                sheet_name="Acumulado_NC",
            )
        except Exception as exc:
            sap_error = str(exc)
            LOGGER.exception("SAP fallo durante la ejecucion")

        try:
            # Ejecuta lotes PostgreSQL y exporta resultado.
            pg_rows, pg_cols = self._ejecutar_por_lotes(
                fecha_inicio_date,
                fecha_fin_date,
                self._postgres_repository,
                "POSTGRES",
                status_cb,
            )
            exportar_excel(pg_rows, pg_cols, self._postgres_output_path)
            # Agrega pestaña de acumulado NC en TUTATI.xlsx.
            pg_nc_rows, pg_nc_cols = self._ejecutar_por_lotes(
                fecha_inicio_date,
                fecha_fin_date,
                self._postgres_repository,
                "POSTGRES_NC",
                status_cb,
                query_method_name="ejecutar_consulta_nc_sql",
            )
            pg_nc_acum_rows, pg_nc_acum_cols = _acumular_tutati_nc(pg_nc_rows, pg_nc_cols)
            exportar_pestana_excel(
                pg_nc_acum_rows,
                pg_nc_acum_cols,
                self._postgres_output_path,
                sheet_name="Acumulado_NC",
            )
        except Exception as exc:
            pg_error = str(exc)
            LOGGER.exception("PostgreSQL fallo durante la ejecucion")

        if sap_error and pg_error:
            # Si ambos fallan, se informa error global.
            raise RuntimeError(
                f"SAP fallo: {sap_error} | PostgreSQL fallo: {pg_error}"
            )

        # Genera comparacion solo si ambas fuentes se exportaron.
        comparacion_error: str | None = None
        comparacion_nc_error: str | None = None
        if (not sap_error) and (not pg_error) and sap_cols and pg_cols:
            try:
                self._generar_comparacion(
                    sap_rows,
                    sap_cols,
                    pg_rows,
                    pg_cols,
                    sheet_name="Comparacion",
                )
            except Exception as exc:
                comparacion_error = str(exc)
                LOGGER.exception("Comparacion fallo durante la ejecucion")
        else:
            comparacion_error = "Comparacion omitida por error previo en SAP o PostgreSQL."

        # Genera comparacion NC en otra pestaña.
        if (not sap_error) and (not pg_error) and sap_nc_cols and pg_nc_cols:
            try:
                self._generar_comparacion(
                    sap_nc_rows,
                    sap_nc_cols,
                    pg_nc_rows,
                    pg_nc_cols,
                    sheet_name="Comparacion_NC",
                )
            except Exception as exc:
                comparacion_nc_error = str(exc)
                LOGGER.exception("Comparacion NC fallo durante la ejecucion")
        else:
            comparacion_nc_error = "Comparacion NC omitida por error previo en SAP o PostgreSQL."

        # Retorna resumen para mostrar en UI.
        return {
            "sap": len(sap_rows),
            "postgres": len(pg_rows),
            "sap_error": sap_error,
            "postgres_error": pg_error,
            "comparacion_error": comparacion_error,
            "comparacion_nc_error": comparacion_nc_error,
        }

    def probar_conexiones(self) -> dict[str, str]:
        # Test rapido de conectividad sin correr consultas pesadas.
        sap = "OK"
        pg = "OK"
        try:
            self._sap_repository.probar_conexion()
        except Exception as exc:
            sap = str(exc)
        try:
            self._postgres_repository.probar_conexion()
        except Exception as exc:
            pg = str(exc)
        return {"sap": sap, "postgres": pg}

    def _ejecutar_por_lotes(
        self,
        fecha_inicio_date: date,
        fecha_fin_date: date,
        repository,
        etiqueta: str,
        status_cb=None,
        query_method_name: str = "ejecutar_consulta_sql",
    ) -> tuple[list[tuple[Any, ...]], list[str]]:
        # Ejecuta por dia para evitar consultas demasiado grandes.
        fecha_actual = fecha_inicio_date
        rows_total: list[tuple[Any, ...]] = []
        cols: list[str] | None = None
        paso = 0

        while fecha_actual <= fecha_fin_date:
            paso += 1
            msg = f"{etiqueta} lote {paso}: {fecha_actual} -> {fecha_actual}"
            LOGGER.info(msg)
            if status_cb:
                status_cb(msg)

            rows, cols = getattr(repository, query_method_name)(fecha_actual, fecha_actual)
            rows_total.extend(rows)
            fecha_actual += timedelta(days=1)

        if cols is None:
            raise RuntimeError(f"La consulta {etiqueta} no devolvio estructura de columnas.")

        # Devuelve todo apilado (sin consolidar).
        return rows_total, cols

    def _generar_comparacion(
        self,
        sap_rows: list[tuple[Any, ...]],
        sap_cols: list[str],
        pg_rows: list[tuple[Any, ...]],
        pg_cols: list[str],
        sheet_name: str,
    ) -> None:
        # Busca columnas necesarias por nombre (sin sensibilidad a mayusculas).
        idx_sap_ref = _find_col_index(sap_cols, ["referencia"])
        idx_sap_doc = _find_col_index(sap_cols, ["u_bot_docentry"])
        idx_sap_fecha = _find_col_index_optional(sap_cols, ["fecha"])
        idx_pg_id = _find_col_index(pg_cols, ["eid_orders", "eid"])
        idx_pg_uid = _find_col_index(pg_cols, ["uid_orders", "uid_rmas"])
        idx_pg_fecha = _find_col_index_optional(pg_cols, ["fecha"])
        idx_pg_cuid = _find_col_index_optional(pg_cols, ["cuid_documented"])

        sap_items = [
            {
                "id": _norm_id(row[idx_sap_ref]),
                "doc": str(row[idx_sap_doc]) if row[idx_sap_doc] is not None else "",
                "fecha": str(row[idx_sap_fecha]) if idx_sap_fecha is not None and row[idx_sap_fecha] is not None else "",
            }
            for row in sap_rows
            if _norm_id(row[idx_sap_ref]) != ""
        ]
        pg_items: list[dict[str, str]] = []
        for row in pg_rows:
            item_id = _norm_id(row[idx_pg_id])
            if item_id == "":
                continue
            fecha_pg = ""
            if idx_pg_fecha is not None and row[idx_pg_fecha] is not None:
                fecha_pg = str(row[idx_pg_fecha])
            elif idx_pg_cuid is not None:
                fecha_pg = _fecha_desde_cuid(row[idx_pg_cuid])
            pg_items.append(
                {
                    "id": item_id,
                    "uid": str(row[idx_pg_uid]) if row[idx_pg_uid] is not None else "",
                    "fecha": fecha_pg,
                }
            )

        # Compara por identificador y por cantidad de ocurrencias.
        sap_por_id: dict[str, list[dict[str, str]]] = {}
        for item in sap_items:
            sap_por_id.setdefault(item["id"], []).append(item)
        pg_por_id: dict[str, list[dict[str, str]]] = {}
        for item in pg_items:
            pg_por_id.setdefault(item["id"], []).append(item)

        faltan_en_sap: list[dict[str, str]] = []
        faltan_en_tutati: list[dict[str, str]] = []
        for key in sorted(set(sap_por_id.keys()) | set(pg_por_id.keys())):
            sap_list = sap_por_id.get(key, [])
            pg_list = pg_por_id.get(key, [])
            min_len = min(len(sap_list), len(pg_list))
            if len(pg_list) > min_len:
                faltan_en_sap.extend(pg_list[min_len:])
            if len(sap_list) > min_len:
                faltan_en_tutati.extend(sap_list[min_len:])

        faltantes: list[dict[str, str]] = []
        for item in faltan_en_sap:
            faltantes.append(
                {
                    "tipo_faltante": "FALTA_EN_SAP",
                    "sap": "",
                    "tutati": item["uid"],
                    "fecha": item["fecha"],
                }
            )
        for item in faltan_en_tutati:
            faltantes.append(
                {
                    "tipo_faltante": "FALTA_EN_TUTATI",
                    "sap": item["doc"],
                    "tutati": "",
                    "fecha": item["fecha"],
                }
            )

        # Diferencias de monto por identificador: SUMA (SAP) - TOTAL (TUTATI).
        diferencias = _calcular_diferencias_monto(
            sap_rows=sap_rows,
            sap_cols=sap_cols,
            pg_rows=pg_rows,
            pg_cols=pg_cols,
            threshold=0.12,
        )

        resumen = {
            "sap": len(sap_rows),
            "tutati": len(pg_rows),
            "faltan_en_sap": len(faltan_en_sap),
            "faltan_en_tutati": len(faltan_en_tutati),
        }
        exportar_comparacion(
            resumen,
            faltantes,
            diferencias,
            self._comparacion_output_path,
            sheet_name=sheet_name,
        )


def _find_col_index(cols: list[str], candidates: list[str]) -> int:
    normalized = {c.strip().lower(): i for i, c in enumerate(cols)}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    raise RuntimeError(f"No se encontro columna requerida. Esperadas: {', '.join(candidates)}")


def _norm_id(value: Any) -> str:
    if value is None:
        return ""
    return str(value).strip().upper()


def _to_float(value: Any) -> float:
    if value is None:
        return 0.0
    if isinstance(value, (int, float)):
        return float(value)
    if isinstance(value, Decimal):
        return float(value)
    raw = str(value).strip().replace(" ", "")
    if raw == "":
        return 0.0
    if "," in raw and "." in raw:
        raw = raw.replace(",", "")
    elif "," in raw and "." not in raw:
        raw = raw.replace(",", ".")
    return float(raw)


def _calcular_diferencias_monto(
    sap_rows: list[tuple[Any, ...]],
    sap_cols: list[str],
    pg_rows: list[tuple[Any, ...]],
    pg_cols: list[str],
    threshold: float,
) -> list[dict[str, str | float]]:
    idx_sap_ref = _find_col_index(sap_cols, ["referencia"])
    idx_sap_doc = _find_col_index(sap_cols, ["u_bot_docentry"])
    idx_sap_suma = _find_col_index(sap_cols, ["suma"])
    idx_sap_fecha = _find_col_index_optional(sap_cols, ["fecha"])

    idx_pg_id = _find_col_index(pg_cols, ["eid_orders", "eid"])
    idx_pg_uid = _find_col_index(pg_cols, ["uid_orders", "uid_rmas"])
    idx_pg_total = _find_col_index(pg_cols, ["total"])

    sap_map: dict[str, list[dict[str, Any]]] = {}
    for row in sap_rows:
        key = _norm_id(row[idx_sap_ref])
        if key == "":
            continue
        sap_map.setdefault(key, []).append(
            {
                "u_bot_docentry": str(row[idx_sap_doc]) if row[idx_sap_doc] is not None else "",
                "fecha": str(row[idx_sap_fecha]) if idx_sap_fecha is not None and row[idx_sap_fecha] is not None else "",
                "referencia": key,
                "suma": _to_float(row[idx_sap_suma]),
            }
        )

    pg_map: dict[str, list[dict[str, Any]]] = {}
    for row in pg_rows:
        key = _norm_id(row[idx_pg_id])
        if key == "":
            continue
        pg_map.setdefault(key, []).append(
            {
                "uid_orders": str(row[idx_pg_uid]) if row[idx_pg_uid] is not None else "",
                "eid_orders": key,
                "total": _to_float(row[idx_pg_total]),
            }
        )

    diferencias: list[dict[str, str | float]] = []
    for key in sorted(set(sap_map.keys()) & set(pg_map.keys())):
        sap_list = sap_map[key]
        pg_list = pg_map[key]
        for sap_item, pg_item in zip(sap_list, pg_list):
            diferencia = sap_item["suma"] - pg_item["total"]
            if abs(diferencia) > threshold:
                diferencias.append(
                    {
                        "u_bot_docentry": sap_item["u_bot_docentry"],
                        "uid_orders": pg_item["uid_orders"],
                        "fecha": sap_item["fecha"],
                        "suma_sap": round(sap_item["suma"], 4),
                        "total_tutati": round(pg_item["total"], 4),
                        "diferencia": round(diferencia, 4),
                    }
                )

    return diferencias


def _acumular_sap_nc(
    rows: list[tuple[Any, ...]],
    cols: list[str],
) -> tuple[list[tuple[Any, ...]], list[str]]:
    idx_ref = _find_col_index(cols, ["referencia"])
    idx_linetotal = _find_col_index(cols, ["linetotal"])
    idx_igv = _find_col_index(cols, ["igv"])
    idx_suma = _find_col_index(cols, ["suma"])

    acumulado: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = _norm_id(row[idx_ref])
        if key == "":
            continue
        if key not in acumulado:
            acumulado[key] = {
                "referencia": key,
                "linetotal_acumulado": 0.0,
                "igv_acumulado": 0.0,
                "suma_acumulado": 0.0,
            }
        acumulado[key]["linetotal_acumulado"] += _to_float(row[idx_linetotal])
        acumulado[key]["igv_acumulado"] += _to_float(row[idx_igv])
        acumulado[key]["suma_acumulado"] += _to_float(row[idx_suma])

    data = [
        (
            item["referencia"],
            round(item["linetotal_acumulado"], 4),
            round(item["igv_acumulado"], 4),
            round(item["suma_acumulado"], 4),
        )
        for item in sorted(acumulado.values(), key=lambda x: x["referencia"])
    ]
    return data, [
        "referencia",
        "linetotal_acumulado",
        "igv_acumulado",
        "suma_acumulado",
    ]


def _acumular_tutati_nc(
    rows: list[tuple[Any, ...]],
    cols: list[str],
) -> tuple[list[tuple[Any, ...]], list[str]]:
    idx_eid = _find_col_index(cols, ["eid", "eid_orders"])
    idx_total = _find_col_index(cols, ["total"])
    idx_uid = _find_col_index(cols, ["uid_rmas", "uid_orders"])

    acumulado: dict[str, dict[str, Any]] = {}
    for row in rows:
        key = _norm_id(row[idx_eid])
        if key == "":
            continue
        if key not in acumulado:
            acumulado[key] = {
                "eid": key,
                "total_acumulado": 0.0,
                "uid_referencia": str(row[idx_uid]) if row[idx_uid] is not None else "",
            }
        acumulado[key]["total_acumulado"] += _to_float(row[idx_total])

    data = [
        (
            item["eid"],
            item["uid_referencia"],
            round(item["total_acumulado"], 4),
        )
        for item in sorted(acumulado.values(), key=lambda x: x["eid"])
    ]
    return data, ["eid", "uid_rmas_referencia", "total_acumulado"]


def _find_col_index_optional(cols: list[str], candidates: list[str]) -> int | None:
    normalized = {c.strip().lower(): i for i, c in enumerate(cols)}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    return None


def _fecha_desde_cuid(cuid_value: Any) -> str:
    # Obtiene solo fecha DD-MM-YYYY desde CUID, si es valido.
    if cuid_value is None:
        return ""
    try:
        return cuid_a_fecha(cuid_value).strftime("%d-%m-%Y")
    except Exception:
        return ""
