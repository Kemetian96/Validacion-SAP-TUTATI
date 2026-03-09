from pathlib import Path
import sys


# Agrega `src` al path para ejecutar sin instalar el paquete.
ROOT_DIR = Path(__file__).resolve().parent
SRC_DIR = ROOT_DIR / "src"
if str(SRC_DIR) not in sys.path:
    sys.path.insert(0, str(SRC_DIR))

from sap_report.main import main


# Punto de entrada de la aplicacion.
if __name__ == "__main__":
    main()
