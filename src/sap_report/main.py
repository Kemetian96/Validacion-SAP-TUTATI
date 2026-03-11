from sap_report.application import ReportService
from sap_report.infrastructure import load_settings
from sap_report.infrastructure.db import MySQLRepository, PostgresRepository, SapHanaRepository
from sap_report.logging_config import configure_logging
from sap_report.ui import run_ui


def main() -> None:
    # Inicializa logging global de la app.
    configure_logging()
    # Carga variables de entorno y defaults.
    settings = load_settings()
    # Crea repositorios para ambas fuentes de datos.
    sap_repository = SapHanaRepository(settings)
    postgres_repository = PostgresRepository(settings)
    mysql_repository = MySQLRepository(settings)
    # Crea servicio de negocio que ejecuta ambos reportes.
    service = ReportService(
        sap_repository=sap_repository,
        postgres_repository=postgres_repository,
        mysql_repository=mysql_repository,
        sap_output_path=settings.sap_output_path,
        postgres_output_path=settings.pg_output_path,
        comparacion_output_path=settings.comparacion_output_path,
    )

    # Lanza la interfaz con rango inicial y tamano configurado.
    run_ui(
        service=service,
        fecha_inicio_default_raw=settings.fecha_inicio_default,
        fecha_fin_default_raw=settings.fecha_fin_default,
        ui_width=settings.ui_width,
        ui_height=settings.ui_height,
    )


# Permite ejecutar como script directo.
if __name__ == "__main__":
    main()
