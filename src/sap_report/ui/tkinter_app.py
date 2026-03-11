import os
import shutil
import subprocess
import threading
import tkinter as tk
import webbrowser
from datetime import date, datetime
from tkinter import messagebox, ttk

from sap_report.application import ReportService

try:
    from tkcalendar import DateEntry
except ImportError:
    DateEntry = None


def _parse_env_date(value: str) -> date:
    # Acepta fecha ISO o YYYY-MM-DD.
    value = value.strip()
    try:
        return datetime.fromisoformat(value).date()
    except ValueError:
        return datetime.strptime(value, "%Y-%m-%d").date()


def _find_browser_cmd() -> str | None:
    # Prioriza Chrome y luego Brave en Windows.
    candidates = [
        shutil.which("chrome"),
        shutil.which("chrome.exe"),
        shutil.which("brave"),
        shutil.which("brave.exe"),
    ]
    for cand in candidates:
        if cand:
            return cand

    program_files = os.environ.get("ProgramFiles", r"C:\Program Files")
    program_files_x86 = os.environ.get("ProgramFiles(x86)", r"C:\Program Files (x86)")
    paths = [
        os.path.join(program_files, "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(program_files_x86, "Google", "Chrome", "Application", "chrome.exe"),
        os.path.join(program_files, "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
        os.path.join(program_files_x86, "BraveSoftware", "Brave-Browser", "Application", "brave.exe"),
    ]
    for path in paths:
        if os.path.exists(path):
            return path
    return None


def run_ui(
    service: ReportService,
    fecha_inicio_default_raw: str,
    fecha_fin_default_raw: str,
    ui_width: int,
    ui_height: int,
) -> None:
    # Dependencia de selector de fecha.
    if DateEntry is None:
        raise RuntimeError("Falta dependencia 'tkcalendar'. Instala con: pip install tkcalendar")

    # Valores iniciales del calendario.
    fecha_inicio_default = _parse_env_date(fecha_inicio_default_raw)
    fecha_fin_default = _parse_env_date(fecha_fin_default_raw)

    # Configura ventana principal.
    root = tk.Tk()
    root.title("Reporte SAP")
    root.geometry(f"{ui_width}x{ui_height}")
    root.resizable(False, False)

    try:
        ttk.Style().theme_use("vista")
    except tk.TclError:
        pass

    estado_var = tk.StringVar(value="Selecciona el rango y ejecuta.")

    # Layout principal.
    main = ttk.Frame(root, padding=16)
    main.pack(fill="both", expand=True)

    card = ttk.LabelFrame(main, text="Rango de fechas", padding=12)
    card.pack(fill="x")

    ttk.Label(card, text="Fecha inicio").grid(row=0, column=0, sticky="w", pady=(0, 8))
    fecha_inicio_entry = DateEntry(
        card,
        width=16,
        date_pattern="yyyy-mm-dd",
        year=fecha_inicio_default.year,
        month=fecha_inicio_default.month,
        day=fecha_inicio_default.day,
    )
    fecha_inicio_entry.grid(row=0, column=1, sticky="w", pady=(0, 8), padx=(8, 0))

    ttk.Label(card, text="Fecha fin").grid(row=1, column=0, sticky="w")
    fecha_fin_entry = DateEntry(
        card,
        width=16,
        date_pattern="yyyy-mm-dd",
        year=fecha_fin_default.year,
        month=fecha_fin_default.month,
        day=fecha_fin_default.day,
    )
    fecha_fin_entry.grid(row=1, column=1, sticky="w", padx=(8, 0))

    btn_row = ttk.Frame(main)
    btn_row.pack(fill="x", pady=(14, 0))
    ejecutar_btn = ttk.Button(btn_row, text="Ejecutar reporte", width=17)
    ejecutar_btn.pack(side="left")
    probar_btn = ttk.Button(btn_row, text="Probar conexion", width=17)
    probar_btn.pack(side="left", padx=(8, 0))

    btn_row_2 = ttk.Frame(main)
    btn_row_2.pack(fill="x", pady=(8, 0))
    validar_btn = ttk.Button(btn_row_2, text="Validar Articulos", width=17)
    validar_btn.pack(side="left")
    validar_igv_btn = ttk.Button(btn_row_2, text="Validar Igv", width=17)
    validar_igv_btn.pack(side="left", padx=(8, 0))

    ttk.Label(main, textvariable=estado_var).pack(anchor="w", pady=(14, 0))

    running = {"value": False}

    def set_estado(msg: str) -> None:
        # Actualiza etiqueta de estado desde cualquier hilo.
        root.after(0, lambda: estado_var.set(msg))

    def on_run() -> None:
        # Evita ejecuciones simultaneas.
        if running["value"]:
            return

        fecha_inicio = fecha_inicio_entry.get_date()
        fecha_fin = fecha_fin_entry.get_date()

        running["value"] = True
        ejecutar_btn.state(["disabled"])
        set_estado("Ejecutando consulta...")

        def worker() -> None:
            # Ejecuta en segundo plano para no congelar UI.
            try:
                totals = service.ejecutar_reporte(
                    fecha_inicio_date=fecha_inicio,
                    fecha_fin_date=fecha_fin,
                    status_cb=set_estado,
                )
                root.after(
                    0,
                    lambda: messagebox.showinfo(
                        "Ejecucion completada",
                        "SAP filas: {sap}\nPostgreSQL filas: {pg}\n\n"
                        "SAP error: {sap_err}\nPostgreSQL error: {pg_err}\nComparacion: {comp_err}\nComparacion NC: {comp_nc_err}".format(
                            sap=totals["sap"],
                            pg=totals["postgres"],
                            sap_err=totals["sap_error"] or "OK",
                            pg_err=totals["postgres_error"] or "OK",
                            comp_err=totals["comparacion_error"] or "OK",
                            comp_nc_err=totals["comparacion_nc_error"] or "OK",
                        ),
                    ),
                )
                set_estado("Proceso completado.")
            except Exception as exc:
                root.after(0, lambda: messagebox.showerror("Error", str(exc)))
                set_estado(f"Error: {exc}")
            finally:
                running["value"] = False
                root.after(0, lambda: ejecutar_btn.state(["!disabled"]))

        threading.Thread(target=worker, daemon=True).start()

    def on_test() -> None:
        # Prueba conexiones SAP/PostgreSQL sin ejecutar reporte.
        if running["value"]:
            return
        running["value"] = True
        ejecutar_btn.state(["disabled"])
        probar_btn.state(["disabled"])
        set_estado("Probando conexiones...")

        def worker_test() -> None:
            # Test en background para mantener interfaz fluida.
            try:
                result = service.probar_conexiones()
                root.after(
                    0,
                    lambda: messagebox.showinfo(
                        "Resultado conexiones",
                        f"SAP: {result['sap']}\nPostgreSQL: {result['postgres']}",
                    ),
                )
                set_estado("Prueba de conexiones completada.")
            except Exception as exc:
                root.after(0, lambda: messagebox.showerror("Error", str(exc)))
                set_estado(f"Error: {exc}")
            finally:
                running["value"] = False
                root.after(0, lambda: ejecutar_btn.state(["!disabled"]))
                root.after(0, lambda: probar_btn.state(["!disabled"]))

        threading.Thread(target=worker_test, daemon=True).start()

    def on_validar() -> None:
        # Ejecuta validacion de articulos sin bloquear la UI.
        if running["value"]:
            return
        running["value"] = True
        ejecutar_btn.state(["disabled"])
        probar_btn.state(["disabled"])
        validar_btn.state(["disabled"])
        set_estado("Validando articulos...")

        def worker_validar() -> None:
            try:
                urls = service.validar_articulos(status_cb=set_estado)
                if urls:
                    browser_cmd = _find_browser_cmd()
                    if browser_cmd:
                        subprocess.Popen([browser_cmd, "--guest", "--new-window", urls[0]])
                        for url in urls[1:]:
                            subprocess.Popen([browser_cmd, "--guest", "--new-tab", url])
                    else:
                        webbrowser.open_new(urls[0])
                        for url in urls[1:]:
                            webbrowser.open_new_tab(url)
                root.after(
                    0,
                    lambda: messagebox.showinfo(
                        "Validar Articulos",
                        f"Se abrieron {len(urls)} URLs en el navegador.",
                    ),
                )
                set_estado("Validacion completada.")
            except Exception as exc:
                root.after(0, lambda: messagebox.showerror("Error", str(exc)))
                set_estado(f"Error: {exc}")
            finally:
                running["value"] = False
                root.after(0, lambda: ejecutar_btn.state(["!disabled"]))
                root.after(0, lambda: probar_btn.state(["!disabled"]))
                root.after(0, lambda: validar_btn.state(["!disabled"]))

        threading.Thread(target=worker_validar, daemon=True).start()

    def on_validar_igv() -> None:
        # Ejecuta validacion IGV sin bloquear la UI.
        if running["value"]:
            return
        running["value"] = True
        ejecutar_btn.state(["disabled"])
        probar_btn.state(["disabled"])
        validar_btn.state(["disabled"])
        validar_igv_btn.state(["disabled"])
        set_estado("Validando IGV...")

        def worker_igv() -> None:
            try:
                resumen = service.validar_igv(status_cb=set_estado)
                root.after(
                    0,
                    lambda: messagebox.showinfo(
                        "Validar Igv",
                        "Items total: {items_total}\nItems IGV: {items_igv}\n"
                        "Actualizados Comercial: {upd_comercial}\nActualizados Pedral: {upd_pedral}".format(
                            items_total=resumen["items_total"],
                            items_igv=resumen["items_igv"],
                            upd_comercial=resumen["upd_comercial"],
                            upd_pedral=resumen["upd_pedral"],
                        ),
                    ),
                )
                set_estado("Validacion IGV completada.")
            except Exception as exc:
                root.after(0, lambda: messagebox.showerror("Error", str(exc)))
                set_estado(f"Error: {exc}")
            finally:
                running["value"] = False
                root.after(0, lambda: ejecutar_btn.state(["!disabled"]))
                root.after(0, lambda: probar_btn.state(["!disabled"]))
                root.after(0, lambda: validar_btn.state(["!disabled"]))
                root.after(0, lambda: validar_igv_btn.state(["!disabled"]))

        threading.Thread(target=worker_igv, daemon=True).start()

    ejecutar_btn.configure(command=on_run)
    probar_btn.configure(command=on_test)
    validar_btn.configure(command=on_validar)
    validar_igv_btn.configure(command=on_validar_igv)
    # Inicia el loop de eventos de Tkinter.
    root.mainloop()
