"""
Asistente COFA v3 — Farmacia Merlo
Servidor local en localhost:7734
El usuario lo ejecuta antes de usar la sección de débitos PAMI.
"""

import http.server
import json
import threading
import urllib.request
import urllib.parse
import urllib.error
import ssl
import sys
import os
import time
import re
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler

VERSION = "3.0.0"
PORT = 7734

# ── Verificar que no haya otra instancia corriendo ───────────────────────────
def puerto_ocupado():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("localhost", PORT))
        s.close()
        return False
    except OSError:
        return True

# ── Estado global ────────────────────────────────────────────────────────────
_scraping_en_progreso = False
_ultimo_resultado = None

# ── SSL sin verificación ─────────────────────────────────────────────────────
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE


def cofa_request(url, method="GET", data=None, cookies=None, referer=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-419,es;q=0.9",
    }
    if cookies:
        headers["Cookie"] = cookies
    if referer:
        headers["Referer"] = referer
    if data:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        body = urllib.parse.urlencode(data).encode("utf-8")
    else:
        body = None

    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=30) as resp:
            return {"status": resp.status, "url": resp.url,
                    "body": resp.read().decode("utf-8", errors="replace"),
                    "headers": dict(resp.headers)}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "url": url,
                "body": e.read().decode("utf-8", errors="replace"), "headers": {}}
    except Exception as e:
        return {"status": 0, "url": url, "body": str(e), "headers": {}}


def parsear_ajustes(html):
    ajuste_links = re.findall(r'<u>(\dQ\d{4})</u>', html)
    monto_total = 0.0
    lines = html.upper().split('\n')
    for i, line in enumerate(lines):
        if 'AJUSTE' in line and 'DEBITO' in line:
            contexto = ' '.join(lines[max(0, i-2):i+5])
            numeros = re.findall(r'[\d]+\.[\d]+,[\d]+', contexto)
            for n in numeros:
                try:
                    val = float(n.replace('.', '').replace(',', '.'))
                    if val > monto_total:
                        monto_total = val
                except:
                    pass
    return ajuste_links, monto_total


def parsear_archivos_ajuste(html):
    archivos = []
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE)
    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
        if len(cells) < 4:
            continue
        nombre = re.sub(r'<[^>]+>', '', cells[1]).strip()
        nota = re.sub(r'<[^>]+>', '', cells[3]).strip()
        if not nombre.endswith('.png'):
            continue
        base = re.sub(r'_00[12]\.png$', '', nombre)
        if base and not any(a['nombre'] == base for a in archivos):
            archivos.append({'nombre': base, 'nota': nota})
    return archivos


def abrir_login_cofa(periodo):
    global _scraping_en_progreso, _ultimo_resultado

    _scraping_en_progreso = True
    _ultimo_resultado = None

    driver = None
    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.common.exceptions import WebDriverException

        opts = Options()
        opts.add_argument("--start-maximized")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        # NO headless — el usuario tiene que ver la ventana

        print("Abriendo Chrome para login en COFA...")

        try:
            driver = webdriver.Chrome(options=opts)
        except WebDriverException:
            exe_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(os.path.abspath(__file__))
            service = Service(os.path.join(exe_dir, 'chromedriver.exe'))
            driver = webdriver.Chrome(service=service, options=opts)

        driver.get("https://principal.cofa.org.ar/")
        print("Esperando que el usuario haga login...")

        # Esperar hasta 3 minutos
        timeout = 180
        start = time.time()
        logueado = False

        while time.time() - start < timeout:
            time.sleep(2)
            try:
                current_url = driver.current_url
                page_source = driver.page_source
                if ("Farmacias" in current_url or
                    "tablero" in current_url or
                    ("PAMI" in page_source and "TxtFarmacia" not in page_source)):
                    logueado = True
                    break
            except:
                break

        if not logueado:
            print("Timeout de login.")
            _ultimo_resultado = {"error": "Timeout — el login tardó demasiado"}
            return

        print("Login detectado. Navegando al tablero de PAMI...")

        # Navegar al tablero de resumen
        driver.get("https://ncr.cofa.org.ar/tablero/resumen/")
        time.sleep(3)

        # Verificar que llegamos al tablero
        if "servicios.cofa.org.ar" in driver.current_url:
            print("No se pudo acceder al tablero NCR.")
            _ultimo_resultado = {"error": "No se pudo acceder al tablero. Intentá nuevamente."}
            return

        # Seleccionar período y obtener HTML
        try:
            anio, mes, q = periodo.split("|")
            driver.execute_script(f"""
                var sel = document.querySelector('select');
                if (sel) {{
                    sel.value = '{periodo}';
                    sel.dispatchEvent(new Event('change', {{bubbles: true}}));
                }}
            """)
            time.sleep(2)
        except:
            pass

        # Capturar HTML del resumen
        html_resumen = driver.page_source
        print(f"HTML capturado: {len(html_resumen)} chars")

        # Obtener cookies para requests posteriores
        cookies = driver.get_cookies()
        cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies])
        print(f"Cookies: {cookie_str[:60]}...")

        # Parsear ajustes del HTML
        ajuste_links, monto_total = parsear_ajustes(html_resumen)
        print(f"Ajustes encontrados: {ajuste_links}, monto: {monto_total}")

        if not ajuste_links:
            _ultimo_resultado = {"periodo": periodo, "ajustes": [], "total_recetas": 0, "total_monto": 0}
            return

        # Hacer click en cada ajuste y leer el iframe
        ajustes = []
        for link_id in ajuste_links:
            print(f"Cargando ajuste {link_id}...")
            try:
                # Click en el link del ajuste
                driver.execute_script(f"""
                    var els = document.querySelectorAll('u');
                    for (var e of els) {{
                        if (e.textContent.trim() === '{link_id}') {{ e.click(); break; }}
                    }}
                """)
                time.sleep(2)

                # Leer el iframe
                try:
                    iframe = driver.find_element("name", "frameC")
                    driver.switch_to.frame(iframe)
                    html_ajuste = driver.page_source
                    driver.switch_to.default_content()
                except:
                    # Fallback: request directo con cookies
                    r_aj = cofa_request(
                        f"https://ncr.cofa.org.ar/tablero/resumen/Ajustes/?ID={link_id}",
                        cookies=cookie_str
                    )
                    html_ajuste = r_aj["body"]

                archivos = parsear_archivos_ajuste(html_ajuste)
                print(f"  Archivos en {link_id}: {len(archivos)}")
            except Exception as e:
                print(f"  Error en {link_id}: {e}")
                archivos = []

            monto_ajuste = round(monto_total / len(ajuste_links), 2)
            ajustes.append({"id": link_id, "monto": monto_ajuste, "archivos": archivos})

        total_recetas = sum(len(a["archivos"]) for a in ajustes)
        print(f"Scraping completo: {total_recetas} recetas en {len(ajustes)} ajuste(s)")

        _ultimo_resultado = {
            "periodo": periodo,
            "ajustes": ajustes,
            "total_recetas": total_recetas,
            "total_monto": monto_total
        }

    except ImportError:
        _ultimo_resultado = {"error": "Selenium no encontrado. Reinstalá el Asistente."}
    except Exception as e:
        print(f"Error: {e}")
        _ultimo_resultado = {"error": str(e)}
    finally:
        _scraping_en_progreso = False
        if driver:
            try:
                driver.quit()
                print("Chrome cerrado.")
            except:
                pass


class CofaHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass  # No loguear cada request

    def send_cors_headers(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_cors_headers()
        self.end_headers()

    def do_GET(self):
        if self.path == "/ping":
            self.respond({"status": "ok", "version": VERSION})
        elif self.path == "/resultado":
            if _scraping_en_progreso:
                self.respond({"estado": "en_progreso"})
            elif _ultimo_resultado is not None:
                self.respond({"estado": "listo", "datos": _ultimo_resultado})
            else:
                self.respond({"estado": "esperando"})
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8")
        try:
            params = json.loads(body)
        except:
            params = {}

        if self.path == "/iniciar-login":
            periodo = params.get("periodo", "")
            if not periodo:
                self.respond({"error": "Falta el período"})
                return
            if _scraping_en_progreso:
                self.respond({"estado": "ya_en_progreso"})
                return
            # Lanzar en thread separado para no bloquear el servidor
            t = threading.Thread(target=abrir_login_cofa, args=(periodo,), daemon=True)
            t.start()
            self.respond({"estado": "iniciado"})
        else:
            self.send_response(404)
            self.end_headers()

    def respond(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_cors_headers()
        self.end_headers()
        self.wfile.write(body)


def main():
    # Verificar que no haya otra instancia
    if puerto_ocupado():
        print(f"El Asistente COFA ya está corriendo en el puerto {PORT}.")
        print("Cerrá la instancia anterior antes de abrir una nueva.")
        input("Presioná Enter para salir...")
        sys.exit(0)

    # Instalar selenium si no está
    try:
        import selenium
        print(f"Selenium disponible.")
    except ImportError:
        print("Instalando Selenium (solo la primera vez)...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "selenium", "--quiet"], check=True)
        print("Selenium instalado.")

    print(f"=" * 50)
    print(f"  Asistente COFA v{VERSION}")
    print(f"  Corriendo en localhost:{PORT}")
    print(f"  Dejá esta ventana abierta mientras usás la app")
    print(f"  Para cerrar: cerrá esta ventana")
    print(f"=" * 50)

    server = HTTPServer(("localhost", PORT), CofaHandler)

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("\nCerrando Asistente COFA...")
        server.shutdown()


if __name__ == "__main__":
    main()
