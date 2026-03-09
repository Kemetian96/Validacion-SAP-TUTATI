from datetime import datetime, timedelta


def fecha_a_cuid(fecha: datetime) -> int:
    # Ajusta al horario del servidor y serializa a entero CUID.
    fecha_servidor = fecha + timedelta(hours=5)
    return int(fecha_servidor.strftime("%Y%m%d%H%M%S") + "00000")
