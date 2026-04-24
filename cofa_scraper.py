"""
Asistente COFA — Farmacia Merlo
Corre como servidor local en localhost:7734
La app web se comunica con este servidor para hacer scraping de COFA
sin restricciones de CORS.
"""

import http.server
import json
import threading
import webbrowser
import urllib.request
import urllib.parse
import urllib.error
import ssl
import sys
import os
import time
from http.server import HTTPServer, BaseHTTPRequestHandler

VERSION = "1.0.0"
PORT = 7734

# ── SSL sin verificación (para sitios con certificados viejos) ──────────────
ssl_ctx = ssl.create_default_context()
ssl_ctx.check_hostname = False
ssl_ctx.verify_mode = ssl.CERT_NONE

def cofa_request(url, method="GET", data=None, cookies=None, referer=None):
    """Hace un request a COFA con las cookies del usuario."""
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
        return {
            "status": e.code,
            "url": url,
            "body": e.read().decode("utf-8", errors="replace"),
            "headers": {}
        }
    except Exception as e:
        return {"status": 0, "url": url, "body": str(e), "headers": {}}


def parsear_ajustes(html):
    """Extrae los links de ajuste y el monto total del HTML del resumen."""
    import re
    ajuste_links = re.findall(r'<u>(\dQ\d{4})</u>', html)
    
    # Buscar montos en fila AJUSTE/DEBITO
    monto_total = 0.0
    lines = html.upper().split('\n')
    for i, line in enumerate(lines):
        if 'AJUSTE' in line and 'DEBITO' in line:
            # Buscar números en las líneas cercanas
            contexto = ' '.join(lines[max(0,i-2):i+5])
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
    """Extrae los archivos PNG y sus notas de error del HTML del ajuste."""
    import re
    archivos = []
    
    # Buscar filas de la tabla
    rows = re.findall(r'<tr[^>]*>(.*?)</tr>', html, re.DOTALL | re.IGNORECASE)
    for row in rows:
        cells = re.findall(r'<td[^>]*>(.*?)</td>', row, re.DOTALL | re.IGNORECASE)
        if len(cells) < 4:
            continue
        # Limpiar HTML de las celdas
        nombre = re.sub(r'<[^>]+>', '', cells[1]).strip()
        nota = re.sub(r'<[^>]+>', '', cells[3]).strip()
        if not nombre.endswith('.png'):
            continue
        # Quitar sufijo _001/_002
        base = re.sub(r'_00[12]\.png$', '', nombre)
        if base and not any(a['nombre'] == base for a in archivos):
            archivos.append({'nombre': base, 'nota': nota})
    
    return archivos


# Cookie de sesión global (se actualiza con cada login exitoso)
_session_cookie_ncr = ""

class CofaHandler(BaseHTTPRequestHandler):
    
    def _get_session_cookie(self):
        return _session_cookie_ncr
    
    def log_message(self, format, *args):
        pass  # Silenciar logs del servidor
    
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
            self.send_response(200)
            self.send_header("Content-Type", "application/json")
            self.send_cors_headers()
            self.end_headers()
            self.wfile.write(json.dumps({"status": "ok", "version": VERSION}).encode())
        else:
            self.send_response(404)
            self.end_headers()
    
    def do_POST(self):
        content_length = int(self.headers.get("Content-Length", 0))
        body = self.rfile.read(content_length).decode("utf-8")
        
        try:
            params = json.loads(body)
        except:
            params = dict(urllib.parse.parse_qsl(body))
        
        if self.path == "/login":
            self.handle_login(params)
        elif self.path == "/scrape":
            self.handle_scrape(params)
        else:
            self.send_response(404)
            self.end_headers()
    
    def handle_login(self, params):
        """Hace login en COFA y devuelve las cookies de sesión."""
        farmacia = params.get("farmacia", "")
        clave = params.get("clave", "")
        
        # 1. GET inicial para obtener cookie de sesión
        r1 = cofa_request("https://principal.cofa.org.ar/")
        cookie_inicial = r1["headers"].get("set-cookie", "").split(";")[0]
        
        # 2. POST login — el recaptcha v3 es invisible y el servidor lo acepta
        # con score bajo cuando viene de un cliente local (no headless browser)
        r2 = cofa_request(
            "https://principal.cofa.org.ar/?",
            method="POST",
            data={
                "Farmacia": farmacia,
                "Clave": clave,
                "recaptcha_response": "",
                "B1": "   Ingresar   "
            },
            cookies=cookie_inicial,
            referer="https://principal.cofa.org.ar/"
        )
        
        # 3. Verificar si llegamos al tablero
        login_ok = "Farmacias" in r2["url"] or "tablero" in r2["url"] or "ncr.cofa" in r2["url"]
        
        # 4. Extraer cookies de la respuesta
        cookie_ncr = r2["headers"].get("set-cookie", "").split(";")[0]
        
        # 5. Si no llegamos, intentar navegar directo al tablero
        if not login_ok:
            r3 = cofa_request(
                "https://principal.cofa.org.ar/Farmacias/",
                cookies=cookie_inicial
            )
            cookie_ncr = r3["headers"].get("set-cookie", cookie_ncr).split(";")[0]
        
        # Guardar la cookie NCR globalmente para usarla en scrape
        global _session_cookie_ncr
        if cookie_ncr:
            _session_cookie_ncr = cookie_ncr.split("=", 1)[-1] if "=" in cookie_ncr else cookie_ncr

        result = {
            "login_ok": login_ok,
            "url_final": r2["url"],
            "cookie_principal": cookie_inicial,
            "cookie_ncr": cookie_ncr,
        }
        
        self.respond(result)
    
    def handle_scrape(self, params):
        """Extrae los débitos del período indicado. Usa la sesión activa del usuario."""
        cookie_ncr = params.get("cookie_ncr", self._get_session_cookie())
        periodo = params.get("periodo", "")
        
        if not periodo:
            self.respond({"error": "Falta el período"}, 200)
            return
        
        if not cookie_ncr:
            self.respond({"error": "No hay sesion activa. Por favor hacé login en COFA primero."}, 200)
            return
        
        # 1. POST al resumen con el período
        r = cofa_request(
            "https://ncr.cofa.org.ar/tablero/resumen/",
            method="POST",
            data={"PeriodoX": periodo},
            cookies=cookie_ncr,
            referer="https://ncr.cofa.org.ar/tablero/"
        )
        
        if "servicios.cofa.org.ar" in r["url"] or r["status"] != 200:
            self.respond({"error": "Sesion expirada o usuario no logueado", "url": r["url"]}, 200)
            return
        
        # 2. Parsear ajustes
        ajuste_links, monto_total = parsear_ajustes(r["body"])
        
        if not ajuste_links:
            self.respond({"periodo": periodo, "ajustes": [], "total_recetas": 0, "total_monto": 0})
            return
        
        # 3. Por cada ajuste, obtener los archivos
        ajustes = []
        for link_id in ajuste_links:
            r_aj = cofa_request(
                f"https://ncr.cofa.org.ar/tablero/resumen/Ajustes/?ID={link_id}",
                cookies=cookie_ncr,
                referer="https://ncr.cofa.org.ar/tablero/resumen/"
            )
            archivos = parsear_archivos_ajuste(r_aj["body"])
            monto_ajuste = round(monto_total / len(ajuste_links), 2)
            ajustes.append({"id": link_id, "monto": monto_ajuste, "archivos": archivos})
        
        total_recetas = sum(len(a["archivos"]) for a in ajustes)
        
        self.respond({
            "periodo": periodo,
            "ajustes": ajustes,
            "total_recetas": total_recetas,
            "total_monto": monto_total
        })
    
    def respond(self, data, status=200):
        body = json.dumps(data, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json")
        self.send_header("Content-Length", str(len(body)))
        self.send_cors_headers()
        self.end_headers()
        self.wfile.write(body)


def main():
    server = HTTPServer(("localhost", PORT), CofaHandler)
    print(f"Asistente COFA corriendo en puerto {PORT}")
    
    # Abrir notificación en el browser (opcional)
    # webbrowser.open(f"http://localhost:{PORT}/ping")
    
    try:
        server.serve_forever()
    except KeyboardInterrupt:
        print("Cerrando...")
        server.shutdown()


if __name__ == "__main__":
    main()
