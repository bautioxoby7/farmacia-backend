"""
Asistente COFA v4 — Farmacia Merlo
Servidor local simple en localhost:7734
Sin Selenium — solo requests HTTP con la cookie del usuario.
"""

import http.server
import json
import urllib.request
import urllib.parse
import urllib.error
import ssl
import sys
import re
import socket
from http.server import HTTPServer, BaseHTTPRequestHandler

VERSION = "4.0.0"
PORT = 7734

ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE


def puerto_libre():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    try:
        s.bind(("localhost", PORT))
        s.close()
        return True
    except OSError:
        return False


def cofa_request(url, method="GET", data=None, cookies=None):
    headers = {
        "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36",
        "Accept": "text/html,application/xhtml+xml,application/xml;q=0.9,*/*;q=0.8",
        "Accept-Language": "es-419,es;q=0.9",
        "Referer": "https://ncr.cofa.org.ar/tablero/",
    }
    if cookies:
        headers["Cookie"] = cookies
    if data:
        headers["Content-Type"] = "application/x-www-form-urlencoded"
        body = urllib.parse.urlencode(data).encode("utf-8")
    else:
        body = None

    req = urllib.request.Request(url, data=body, headers=headers, method=method)
    try:
        with urllib.request.urlopen(req, context=ssl_ctx, timeout=30) as resp:
            return {"status": resp.status, "url": resp.url,
                    "body": resp.read().decode("utf-8", errors="replace")}
    except urllib.error.HTTPError as e:
        return {"status": e.code, "url": url,
                "body": e.read().decode("utf-8", errors="replace")}
    except Exception as e:
        return {"status": 0, "url": url, "body": str(e)}


def parsear_ajustes(html):
    ajuste_links = re.findall(r'<u>(\dQ\d{4})</u>', html)
    monto_total = 0.0
    lines = html.upper().split('\n')
    for i, line in enumerate(lines):
        if 'AJUSTE' in line and 'DEBITO' in line:
            contexto = ' '.join(lines[max(0, i-2):i+5])
            for n in re.findall(r'[\d]+\.[\d]+,[\d]+', contexto):
                try:
                    val = float(n.replace('.', '').replace(',', '.'))
                    if val > monto_total:
                        monto_total = val
                except:
                    pass
    return ajuste_links, monto_total


def parsear_archivos(html):
    archivos = []
    for row in re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE):
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


class Handler(BaseHTTPRequestHandler):

    def log_message(self, format, *args):
        pass

    def send_cors(self):
        self.send_header("Access-Control-Allow-Origin", "*")
        self.send_header("Access-Control-Allow-Methods", "GET, POST, OPTIONS")
        self.send_header("Access-Control-Allow-Headers", "Content-Type")

    def do_OPTIONS(self):
        self.send_response(200)
        self.send_cors()
        self.end_headers()

    def do_GET(self):
        if self.path == "/ping":
            self.ok({"status": "ok", "version": VERSION})
        else:
            self.send_response(404)
            self.end_headers()

    def do_POST(self):
        length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(length).decode("utf-8")
        try:
            params = json.loads(body)
        except:
            params = {}

        if self.path == "/scrape":
            self.handle_scrape(params)
        else:
            self.send_response(404)
            self.end_headers()

    def handle_scrape(self, params):
        cookie = params.get("cookie", "").strip()
        periodo = params.get("periodo", "").strip()

        if not cookie or not periodo:
            self.ok({"error": "Faltan parámetros: cookie y periodo"})
            return

        print(f"Scraping período {periodo}...")
        cookie_str = f"ASPSESSIONIDQETCCSSC={cookie}"

        # POST al resumen con el período
        r = cofa_request(
            "https://ncr.cofa.org.ar/tablero/resumen/",
            method="POST",
            data={"PeriodoX": periodo},
            cookies=cookie_str
        )

        if "servicios.cofa.org.ar" in r["url"] or r["status"] != 200:
            print(f"Sesión inválida. URL: {r['url']}")
            self.ok({"error": "Sesión expirada. Copiá la cookie nuevamente desde COFA."})
            return

        print(f"Resumen cargado OK. Buscando ajustes...")
        ajuste_links, monto_total = parsear_ajustes(r["body"])
        print(f"Ajustes: {ajuste_links}, monto: {monto_total}")

        if not ajuste_links:
            self.ok({"periodo": periodo, "ajustes": [], "total_recetas": 0, "total_monto": 0})
            return

        ajustes = []
        for link_id in ajuste_links:
            print(f"Cargando {link_id}...")
            r_aj = cofa_request(
                f"https://ncr.cofa.org.ar/tablero/resumen/Ajustes/?ID={link_id}",
                cookies=cookie_str
            )
            archivos = parsear_archivos(r_aj["body"])
            print(f"  {len(archivos)} recetas en {link_id}")
            ajustes.append({
                "id": link_id,
                "monto": round(monto_total / len(ajuste_links), 2),
                "archivos": archivos
            })

        total = sum(len(a["archivos"]) for a in ajustes)
        print(f"Listo: {total} recetas encontradas")

        self.ok({
            "periodo": periodo,
            "ajustes": ajustes,
            "total_recetas": total,
            "total_monto": monto_total
        })

    def ok(self, data):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(200)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_cors()
        self.end_headers()
        self.wfile.write(body)


def main():
    if not puerto_libre():
        print(f"El Asistente COFA ya está corriendo.")
        print("Cerrá la otra ventana antes de abrir una nueva.")
        input("Presioná Enter para salir...")
        sys.exit(0)

    print("=" * 45)
    print(f"  Asistente COFA v{VERSION}")
    print(f"  Puerto: {PORT}")
    print(f"  Dejá esta ventana abierta")
    print(f"  Para cerrar: cerrá esta ventana")
    print("=" * 45)

    server = HTTPServer(("localhost", PORT), Handler)
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Cerrando...")


if __name__ == "__main__":
    main()
