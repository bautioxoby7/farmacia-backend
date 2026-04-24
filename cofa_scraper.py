"""
Asistente COFA v2 — Farmacia Merlo
Corre como servidor local en localhost:7734
Abre Chrome para el login y captura las cookies automáticamente.
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
from http.server import HTTPServer, BaseHTTPRequestHandler

VERSION = "2.0.0"
PORT = 7734

# ── Estado global ────────────────────────────────────────────────────────────
_session_cookie_ncr = ""
_scraping_en_progreso = False
_ultimo_resultado = None
_driver = None  # WebDriver de Selenium

# ── SSL sin verificación ─────────────────────────────────────────────────────
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE


def cofa_request(url, method="GET", data=None, cookies=None, referer=None):
    """Hace un request HTTP a COFA con las cookies provistas."""
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
            return {
                "status": resp.status,
                "url": resp.url,
                "body": resp.read().decode("utf-8", errors="replace"),
                "headers": dict(resp.headers)
            }
    except urllib.error.HTTPError as e:
        return {"status": e.code, "url": url,
                "body": e.read().decode("utf-8", errors="replace"), "headers": {}}
    except Exception as e:
        return {"status": 0, "url": url, "body": str(e), "headers": {}}


def parsear_ajustes(html):
    """Extrae los links de ajuste y el monto total del HTML del resumen."""
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
    """Extrae los archivos PNG y sus notas de error."""
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
    """
    Abre Chrome con Selenium, espera el login del usuario,
    captura las cookies y hace el scraping.
    """
    global _session_cookie_ncr, _scraping_en_progreso, _ultimo_resultado, _driver

    _scraping_en_progreso = True
    _ultimo_resultado = None

    try:
        from selenium import webdriver
        from selenium.webdriver.chrome.options import Options
        from selenium.webdriver.chrome.service import Service
        from selenium.common.exceptions import WebDriverException

        # Configurar Chrome
        opts = Options()
        opts.add_argument("--start-maximized")
        opts.add_experimental_option("excludeSwitches", ["enable-automation"])
        opts.add_experimental_option("useAutomationExtension", False)
        # No headless — el usuario tiene que ver la ventana para loguearse
        
        try:
            _driver = webdriver.Chrome(options=opts)
        except WebDriverException:
            # Intentar con chromedriver en el mismo directorio
            exe_dir = os.path.dirname(sys.executable) if getattr(sys, 'frozen', False) else os.path.dirname(__file__)
            service = Service(os.path.join(exe_dir, 'chromedriver.exe'))
            _driver = webdriver.Chrome(service=service, options=opts)

        _driver.get("https://principal.cofa.org.ar/")

        print("Esperando login del usuario...")

        # Esperar hasta 3 minutos a que el usuario se loguee
        timeout = 180
        start = time.time()
        logueado = False

        while time.time() - start < timeout:
            time.sleep(2)
            current_url = _driver.current_url
            # Detectar login exitoso
            if "Farmacias" in current_url or "tablero" in current_url or "ncr.cofa" in current_url:
                logueado = True
                break
            # También detectar si ya está en la página principal autenticada
            try:
                page_source = _driver.page_source
                if "PAMI" in page_source and "Farmacias" in page_source and "TxtFarmacia" not in page_source:
                    logueado = True
                    break
            except:
                pass

        if not logueado:
            _ultimo_resultado = {"error": "Timeout — el login tardó demasiado"}
            _scraping_en_progreso = False
            _driver.quit()
            _driver = None
            return

        print("Login detectado. Navegando al tablero...")

        # Navegar al tablero si no estamos ahí
        if "ncr.cofa" not in _driver.current_url:
            _driver.get("https://ncr.cofa.org.ar/tablero/")
            time.sleep(2)

        # Extraer las cookies de ncr.cofa.org.ar
        _driver.get("https://ncr.cofa.org.ar/tablero/resumen/")
        time.sleep(2)

        cookies_ncr = _driver.get_cookies()
        cookie_str = "; ".join([f"{c['name']}={c['value']}" for c in cookies_ncr])
        _session_cookie_ncr = cookie_str

        print(f"Cookies capturadas: {cookie_str[:50]}...")

        # Hacer scraping con las cookies
        result = hacer_scraping(periodo, cookie_str)
        _ultimo_resultado = result

    except ImportError:
        _ultimo_resultado = {"error": "Selenium no está instalado"}
    except Exception as e:
        _ultimo_resultado = {"error": str(e)}
    finally:
        _scraping_en_progreso = False
        if _driver:
            try:
                _driver.quit()
            except:
                pass
            _driver = None


def hacer_scraping(periodo, cookie_str):
    """Hace el scraping de COFA con las cookies provistas."""
    r = cofa_request(
        "https://ncr.cofa.org.ar/tablero/resumen/",
        method="POST",
        data={"PeriodoX": periodo},
        cookies=cookie_str,
        referer="https://ncr.cofa.org.ar/tablero/"
    )

    if "servicios.cofa.org.ar" in r["url"] or r["status"] != 200:
        return {"error": "Sesion expirada", "url": r["url"]}

    ajuste_links, monto_total = parsear_ajustes(r["body"])

    if not ajuste_links:
        return {"periodo": periodo, "ajustes": [], "total_recetas": 0, "total_monto": 0}

    ajustes = []
    for link_id in ajuste_links:
        r_aj = cofa_request(
            f"https://ncr.cofa.org.ar/tablero/resumen/Ajustes/?ID={link_id}",
            cookies=cookie_str,
            referer="https://ncr.cofa.org.ar/tablero/resumen/"
        )
        archivos = parsear_archivos_ajuste(r_aj["body"])
        monto_ajuste = round(monto_total / len(ajuste_links), 2)
        ajustes.append({"id": link_id, "monto": monto_ajuste, "archivos": archivos})

    total_recetas = sum(len(a["archivos"]) for a in ajustes)
    return {
        "periodo": periodo,
        "ajustes": ajustes,
        "total_recetas": total_recetas,
        "total_monto": monto_total
    }


class CofaHandler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass

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
            # El frontend consulta si ya terminó el scraping
            if _scraping_en_progreso:
                self.respond({"estado": "en_progreso"})
            elif _ultimo_resultado:
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
            self.handle_iniciar_login(params)
        else:
            self.send_response(404)
            self.end_headers()

    def handle_iniciar_login(self, params):
        """Abre Chrome para el login y empieza el proceso en background."""
        global _scraping_en_progreso, _ultimo_resultado

        periodo = params.get("periodo", "")
        if not periodo:
            self.respond({"error": "Falta el período"})
            return

        if _scraping_en_progreso:
            self.respond({"estado": "ya_en_progreso"})
            return

        # Resetear resultado anterior
        _ultimo_resultado = None

        # Abrir Chrome en un thread separado para no bloquear el servidor
        t = threading.Thread(target=abrir_login_cofa, args=(periodo,), daemon=True)
        t.start()

        self.respond({"estado": "iniciado", "mensaje": "Chrome abierto. Esperando login..."})

    def respond(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_cors_headers()
        self.end_headers()
        self.wfile.write(body)


def main():
    # Instalar selenium si no está
    try:
        import selenium
    except ImportError:
        print("Instalando Selenium...")
        import subprocess
        subprocess.run([sys.executable, "-m", "pip", "install", "selenium", "--quiet"], check=True)
        print("Selenium instalado.")

    server = HTTPServer(("localhost", PORT), CofaHandler)
    print(f"Asistente COFA v{VERSION} corriendo en puerto {PORT}")

    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Cerrando...")
        server.shutdown()


if __name__ == "__main__":
    main()
