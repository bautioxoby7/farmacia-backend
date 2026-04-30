from fastapi import APIRouter, UploadFile, File, Form, HTTPException
from fastapi.responses import JSONResponse
import pandas as pd
import io
import re
import tempfile
import os
from typing import Optional

router = APIRouter(prefix="/cajas", tags=["cajas"])

# ── CAJEROS CONOCIDOS ──────────────────────────────────────────────────────────
# Nombres tal como aparecen en la columna NOM_USUARIO del First
CAJEROS_CONOCIDOS = ["AARON", "CHRISTIAN", "AYLEN", "LAUTARO", "IGNACIO"]

# ── TOLERANCIA PARA MATCH APROXIMADO (%) ─────────────────────────────────────
TOLERANCIA_PCT = 0.02  # 2%

# ── HELPERS ───────────────────────────────────────────────────────────────────

def leer_xlsx(file_bytes: bytes) -> pd.DataFrame:
    """Lee un xlsx/xlsm y devuelve el primer sheet como DataFrame."""
    buf = io.BytesIO(file_bytes)
    try:
        df = pd.read_excel(buf, header=None, engine="openpyxl")
    except Exception:
        buf.seek(0)
        df = pd.read_excel(buf, header=None)
    return df


def extraer_datos_planilla_cajero(file_bytes: bytes, nombre_archivo: str) -> dict:
    """
    Extrae de la planilla de un cajero:
    - nombre: str
    - efectivo_real: float  (Total efectivo — lo que contó)
    - efectivo_teorico: float  (TOTAL CAJA)
    - tarjetas_real: float  (Total cupones tarjetas)
    - tarjetas_teorico: float  (suma débito + crédito calculada)
    - transferencias_teorico: float  (suma col MP)
    - tkt_nuevo_cobrar_tarjeta: float
    - tkt_nuevo_cobrar_transferencia: float
    - tkt_anulado_transferencia: float
    - reintegros: list[dict]  (tabla completa de reintegros/anulaciones)
    """
    df = leer_xlsx(file_bytes)

    # Convertir todo a string para búsqueda, rellenar NaN
    df_str = df.astype(str).fillna("")

    result = {
        "archivo": nombre_archivo,
        "nombre": _extraer_nombre_cajero(df_str, nombre_archivo),
        "efectivo_real": 0.0,
        "efectivo_teorico": 0.0,
        "tarjetas_real": 0.0,
        "tarjetas_teorico": 0.0,
        "transferencias_teorico": 0.0,
        "tkt_nuevo_cobrar_tarjeta": 0.0,
        "tkt_nuevo_cobrar_transferencia": 0.0,
        "tkt_anulado_transferencia": 0.0,
        "reintegros": [],
    }

    # ── Buscar fila de headers de la tabla resumen ────────────────────────────
    # La tabla resumen tiene: Real | Teorico | Diferencia | tkt nuevo a cobrar | TKT anulado
    # Buscamos la fila que contenga "Real" y "Teorico"
    resumen_row = None
    for i, row in df_str.iterrows():
        vals = row.tolist()
        if any("Real" in v for v in vals) and any("Teorico" in v for v in vals):
            resumen_row = i
            break

    if resumen_row is not None:
        # Las filas siguientes son Efectivo, Tarjetas, Transferencia
        for offset in range(1, 5):
            r = resumen_row + offset
            if r >= len(df_str):
                break
            row_vals = df_str.iloc[r].tolist()
            # Detectar qué medio es
            medio = ""
            for v in row_vals:
                v_lower = v.lower()
                if "efectivo" in v_lower:
                    medio = "efectivo"
                    break
                elif "tarjeta" in v_lower:
                    medio = "tarjetas"
                    break
                elif "transfer" in v_lower:
                    medio = "transferencia"
                    break

            if not medio:
                continue

            # Extraer valores numéricos de esa fila (ignorar '-' y strings vacíos)
            nums = []
            for v in row_vals:
                n = _safe_float(v)
                if n is not None and n != 0:
                    nums.append(n)

            # La fila tiene: [medio_label, Real, Teorico, Diferencia, tkt_nuevo, tkt_anulado]
            # Pero la posición exacta depende del cajero. Buscamos por columna relativa
            # al header "Real"
            col_real = _find_col(df_str, resumen_row, "Real")
            col_teorico = _find_col(df_str, resumen_row, "Teorico")
            col_tkt_nuevo = _find_col(df_str, resumen_row, "tkt nuevo")
            col_tkt_anulado = _find_col(df_str, resumen_row, "TKT anulado")

            real_val = _get_num(df, r, col_real)
            teorico_val = _get_num(df, r, col_teorico)
            tkt_nuevo_val = _get_num(df, r, col_tkt_nuevo)
            tkt_anulado_val = _get_num(df, r, col_tkt_anulado)

            if medio == "efectivo":
                result["efectivo_real"] = real_val
                result["efectivo_teorico"] = teorico_val
            elif medio == "tarjetas":
                result["tarjetas_real"] = real_val
                result["tarjetas_teorico"] = teorico_val
                result["tkt_nuevo_cobrar_tarjeta"] = tkt_nuevo_val
            elif medio == "transferencia":
                result["transferencias_teorico"] = teorico_val
                result["tkt_nuevo_cobrar_transferencia"] = tkt_nuevo_val
                result["tkt_anulado_transferencia"] = tkt_anulado_val

    # ── Extraer tabla de reintegros/anulaciones ───────────────────────────────
    result["reintegros"] = _extraer_reintegros(df, df_str)

    # ── Calcular tarjetas_teorico desde columnas de tarjeta debito/credito ────
    # Si no lo pudimos leer del resumen, lo calculamos de la tabla de cupones
    if result["tarjetas_real"] == 0.0:
        # Buscar "Total cupones tarjetas" en el df
        for i, row in df_str.iterrows():
            for j, val in enumerate(row):
                if "Total cupones" in val or "total cupones" in val.lower():
                    # El valor está en la misma fila, columna siguiente o en la fila siguiente
                    n = _get_num(df, i, j + 1)
                    if n == 0:
                        n = _get_num(df, i + 1, j)
                    if n != 0:
                        result["tarjetas_real"] = n
                    break

    return result


def _extraer_nombre_cajero(df_str: pd.DataFrame, nombre_archivo: str) -> str:
    """Extrae el nombre del cajero de la primera celda no vacía o del nombre de archivo."""
    # Intentar leer de la primera fila
    primera = df_str.iloc[0].tolist()
    for v in primera:
        v = v.strip()
        if v and v.lower() not in ("nan", "none", ""):
            # Tomar la primera palabra en mayúsculas
            partes = v.split()
            for p in partes:
                p_upper = p.upper().strip(".,;:")
                if p_upper in CAJEROS_CONOCIDOS:
                    return p_upper
            # Si no matchea exacto, devolver el primer token
            return partes[0].upper() if partes else "DESCONOCIDO"

    # Fallback: sacar del nombre de archivo (ej: "22_04_2026_Lautaro.xlsx")
    partes = nombre_archivo.replace(".xlsx", "").replace(".xlsm", "").split("_")
    for p in partes:
        if p.upper() in CAJEROS_CONOCIDOS:
            return p.upper()
    return partes[-1].upper() if partes else "DESCONOCIDO"


def _find_col(df_str: pd.DataFrame, row_idx: int, texto: str) -> Optional[int]:
    """Busca la columna donde aparece 'texto' en la fila row_idx."""
    for j, val in enumerate(df_str.iloc[row_idx].tolist()):
        if texto.lower() in val.lower():
            return j
    return None


def _get_num(df: pd.DataFrame, row: int, col: Optional[int]) -> float:
    """Lee un valor numérico del DataFrame original en posición (row, col)."""
    if col is None or row >= len(df) or col >= len(df.columns):
        return 0.0
    val = df.iloc[row, col]
    return _safe_float(str(val)) or 0.0


def _safe_float(s: str) -> Optional[float]:
    """Convierte un string a float, devuelve None si no es posible."""
    if not s or s.strip() in ("nan", "None", "-", "", "0"):
        return None
    # Limpiar: quitar $, espacios, puntos de miles
    s = s.strip().replace("$", "").replace(" ", "")
    # Formato argentino: 1.234,56 → 1234.56
    if re.match(r"^\d{1,3}(\.\d{3})*(,\d+)?$", s):
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." in s:
        # Si tiene ambos, el último separador es el decimal
        if s.rfind(",") > s.rfind("."):
            s = s.replace(".", "").replace(",", ".")
        else:
            s = s.replace(",", "")
    elif "," in s:
        s = s.replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None


def _extraer_reintegros(df: pd.DataFrame, df_str: pd.DataFrame) -> list:
    """
    Extrae la tabla de reintegros/anulaciones de la planilla.
    Devuelve lista de dicts con: tipo, tkt_anulado, tkt_nuevo, diferencia, medio_pago
    """
    reintegros = []
    # Buscar header de la tabla: "REINTEGRO/ANULACION"
    header_row = None
    for i, row in df_str.iterrows():
        for j, val in enumerate(row):
            if "REINTEGRO/ANULACION" in val.upper() or "REINTEGRO" in val.upper() and "ANULACION" in val.upper():
                header_row = i
                break
        if header_row is not None:
            break

    if header_row is None:
        return reintegros

    # Mapear columnas del header
    headers = df_str.iloc[header_row].tolist()
    col_tipo = col_tkt_anulado = col_tkt_nuevo = col_diferencia = col_medio = None
    for j, h in enumerate(headers):
        h_lower = h.lower()
        if "reintegro" in h_lower and "anulacion" in h_lower:
            col_tipo = j
        elif "tkt anulado" in h_lower or ("anulado" in h_lower and "devolver" in h_lower):
            col_tkt_anulado = j
        elif "nuevo a cobrar" in h_lower or "tkt nuevo" in h_lower:
            col_tkt_nuevo = j
        elif "diferencia" in h_lower:
            col_diferencia = j
        elif "medio" in h_lower and "pago" in h_lower:
            col_medio = j

    # Leer filas de datos hasta que estén vacías
    for i in range(header_row + 1, min(header_row + 20, len(df))):
        tipo = df_str.iloc[i, col_tipo].strip() if col_tipo is not None else ""
        if not tipo or tipo.lower() in ("nan", "none", ""):
            continue
        if tipo.upper() not in ("REINTEGRO", "ANULACION"):
            continue

        tkt_anulado = _get_num(df, i, col_tkt_anulado)
        tkt_nuevo = _get_num(df, i, col_tkt_nuevo)
        diferencia = _get_num(df, i, col_diferencia)
        medio = df_str.iloc[i, col_medio].strip() if col_medio is not None and col_medio < len(df_str.columns) else ""

        reintegros.append({
            "tipo": tipo.upper(),
            "tkt_anulado": tkt_anulado,
            "tkt_nuevo_cobrar": tkt_nuevo,
            "diferencia": diferencia,
            "medio_pago": medio.upper() if medio else "",
        })

    return reintegros


def leer_reporte_first(file_bytes: bytes) -> pd.DataFrame:
    """
    Lee el reporte del First y devuelve DataFrame con columnas normalizadas.
    Columnas relevantes:
    - NOM_TIPO_MOV, NOM_USUARIO, TT_EFECTIVO, TT_TARJETA_DEBITO,
      TT_TARJETA_DE_CREDITO, TT_MERCADO_PAGO_TRANSFERENCIA
    """
    buf = io.BytesIO(file_bytes)
    df = pd.read_excel(buf, engine="openpyxl")
    # Normalizar nombres de columnas
    df.columns = [str(c).strip().upper() for c in df.columns]
    # Rellenar NaN numéricos con 0
    cols_numericas = [
        "TT_EFECTIVO", "TT_TARJETA_DEBITO", "TT_TARJETA_DE_CREDITO",
        "TT_MERCADO_PAGO_TRANSFERENCIA", "TT_TRANSFERENCIA_BANCARIA",
        "TT_NOTA_DE_RECUPERO"
    ]
    for col in cols_numericas:
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0)
    return df


def leer_reporte_mp(file_bytes: bytes) -> list:
    """
    Lee el reporte de Mercado Pago (Hoja2) y extrae los montos de transferencias recibidas.
    Devuelve lista de dicts: {nombre, monto}
    """
    buf = io.BytesIO(file_bytes)
    # El reporte MP es texto plano en Hoja2
    df = pd.read_excel(buf, sheet_name="Hoja2", header=None, engine="openpyxl")
    df = df.fillna("").astype(str)

    transferencias = []
    rows = df.iloc[:, 0].tolist()

    i = 0
    while i < len(rows):
        val = rows[i].strip()
        # Patrón: nombre (repetido 2 veces), monto, "Transferencia recibida"
        if val and val not in ("", "nan") and i + 3 < len(rows):
            nombre1 = val
            nombre2 = rows[i + 1].strip()
            monto_str = rows[i + 2].strip()
            tipo = rows[i + 3].strip()

            if nombre1 == nombre2 and "recibida" in tipo.lower():
                monto = _safe_float(monto_str)
                if monto and monto > 0:
                    transferencias.append({
                        "nombre": nombre1,
                        "monto": monto
                    })
                    i += 4
                    continue
        i += 1

    return transferencias


# ── FUNCIÓN PRINCIPAL DE CÁLCULO ──────────────────────────────────────────────

def calcular_control_caja(
    planillas: list,          # Lista de dicts de extraer_datos_planilla_cajero
    first_df: pd.DataFrame,   # DataFrame del reporte First
    mp_transferencias: list,  # Lista de dicts {nombre, monto} del reporte MP
    efectivo_por_cajero: dict,  # {CAJERO: monto_contado} — inputs manuales
    cierre_posnet: float,     # G18 — input manual
    tkt_anulado_otro_dia: float,  # Input manual
    pago_nc_papel: float,     # Input manual
) -> dict:
    """
    Calcula el control completo de caja y devuelve el resultado estructurado.
    """

    # ── Consolidar datos de planillas ────────────────────────────────────────
    total_efectivo_teorico = sum(p["efectivo_teorico"] for p in planillas)
    total_tarjetas_teorico = sum(p["tarjetas_teorico"] for p in planillas)
    total_transferencias_teorico = sum(p["transferencias_teorico"] for p in planillas)
    total_tkt_nuevo_tarjeta = sum(p["tkt_nuevo_cobrar_tarjeta"] for p in planillas)
    total_tkt_nuevo_transferencia = sum(p["tkt_nuevo_cobrar_transferencia"] for p in planillas)
    total_tkt_anulado_transferencia = sum(p["tkt_anulado_transferencia"] for p in planillas)

    # ── TARJETAS ─────────────────────────────────────────────────────────────
    # First tarjetas = suma de TT_TARJETA_DEBITO + TT_TARJETA_DE_CREDITO
    # Solo Facturas (no NC)
    facturas = first_df[first_df.get("NOM_TIPO_MOV", pd.Series(dtype=str)).str.contains("Factura", na=False, case=False)] if "NOM_TIPO_MOV" in first_df.columns else first_df
    nc = first_df[first_df.get("NOM_TIPO_MOV", pd.Series(dtype=str)).str.contains("NC", na=False, case=False)] if "NOM_TIPO_MOV" in first_df.columns else pd.DataFrame()

    first_tarjetas_debito = facturas["TT_TARJETA_DEBITO"].sum() if "TT_TARJETA_DEBITO" in facturas.columns else 0
    first_tarjetas_credito = facturas["TT_TARJETA_DE_CREDITO"].sum() if "TT_TARJETA_DE_CREDITO" in facturas.columns else 0
    first_tarjetas_total = first_tarjetas_debito + first_tarjetas_credito

    tarjetas_real_neto = first_tarjetas_total - total_tkt_nuevo_tarjeta
    diferencia_tarjetas = cierre_posnet - tarjetas_real_neto

    # ── EFECTIVO ─────────────────────────────────────────────────────────────
    first_efectivo = facturas["TT_EFECTIVO"].sum() if "TT_EFECTIVO" in facturas.columns else 0

    efectivo_real_sum = sum(efectivo_por_cajero.values())
    efectivo_real_neto = (
        first_efectivo
        - tkt_anulado_otro_dia
        - pago_nc_papel
        + total_tkt_anulado_transferencia
    )
    diferencia_efectivo = efectivo_real_sum - efectivo_real_neto

    # ── TRANSFERENCIAS ───────────────────────────────────────────────────────
    total_mp_real = sum(t["monto"] for t in mp_transferencias)
    transferencias_real_neto = total_mp_real - total_tkt_nuevo_transferencia
    diferencia_transferencias = transferencias_real_neto - total_transferencias_teorico

    # ── DIFERENCIA TOTAL Y % ─────────────────────────────────────────────────
    total_general = first_tarjetas_total + first_efectivo + total_mp_real
    diferencia_total = abs(diferencia_tarjetas) + abs(diferencia_efectivo) + abs(diferencia_transferencias)
    pct_diferencia = (diferencia_total / total_general * 100) if total_general else 0

    # ── CRUCES TICKET POR TICKET ──────────────────────────────────────────────
    cruce_tarjetas = _cruzar_tarjetas(planillas, first_df)
    cruce_efectivo = _cruzar_efectivo(planillas, first_df)
    cruce_transferencias = _cruzar_transferencias(mp_transferencias, first_df)

    return {
        "resumen": {
            "efectivo": {
                "real_cajeros": efectivo_real_sum,
                "real_neto_first": efectivo_real_neto,
                "first_bruto": first_efectivo,
                "tkt_anulado_otro_dia": tkt_anulado_otro_dia,
                "pago_nc_papel": pago_nc_papel,
                "tkt_anulado_transferencia": total_tkt_anulado_transferencia,
                "teorico": total_efectivo_teorico,
                "diferencia": diferencia_efectivo,
            },
            "tarjetas": {
                "first_total": first_tarjetas_total,
                "first_debito": first_tarjetas_debito,
                "first_credito": first_tarjetas_credito,
                "tkt_nuevo_cobrar": total_tkt_nuevo_tarjeta,
                "real_neto": tarjetas_real_neto,
                "cierre_posnet": cierre_posnet,
                "teorico": total_tarjetas_teorico,
                "diferencia": diferencia_tarjetas,
            },
            "transferencias": {
                "mp_real": total_mp_real,
                "tkt_nuevo_cobrar": total_tkt_nuevo_transferencia,
                "real_neto": transferencias_real_neto,
                "teorico": total_transferencias_teorico,
                "diferencia": diferencia_transferencias,
            },
            "total": {
                "diferencia_absoluta": diferencia_total,
                "total_general": total_general,
                "pct_diferencia": round(pct_diferencia, 2),
                "estado": "OK" if pct_diferencia <= 1.5 else "REVISAR",
            }
        },
        "por_cajero": _resumen_por_cajero(planillas, efectivo_por_cajero),
        "cruces": {
            "tarjetas": cruce_tarjetas,
            "efectivo": cruce_efectivo,
            "transferencias": cruce_transferencias,
        }
    }


def _resumen_por_cajero(planillas: list, efectivo_por_cajero: dict) -> list:
    resultado = []
    for p in planillas:
        nombre = p["nombre"]
        ef_real = efectivo_por_cajero.get(nombre, 0)
        resultado.append({
            "cajero": nombre,
            "efectivo_contado": ef_real,
            "efectivo_teorico": p["efectivo_teorico"],
            "tarjetas_real": p["tarjetas_real"],
            "tarjetas_teorico": p["tarjetas_teorico"],
            "transferencias_teorico": p["transferencias_teorico"],
        })
    return resultado


def _cruzar_tarjetas(planillas: list, first_df: pd.DataFrame) -> dict:
    """
    Cruza tarjetas ticket por ticket, cajero por cajero.
    Planillas vs First (TT_TARJETA_DEBITO + TT_TARJETA_DE_CREDITO).
    """
    resultado = {}

    for p in planillas:
        cajero = p["nombre"]
        # Tickets de tarjeta del cajero en el First
        if "NOM_USUARIO" not in first_df.columns:
            continue
        first_cajero = first_df[
            first_df["NOM_USUARIO"].str.strip().str.upper() == cajero
        ].copy()
        first_cajero = first_cajero[
            first_cajero.get("NOM_TIPO_MOV", pd.Series(dtype=str)).str.contains("Factura", na=False, case=False)
        ] if "NOM_TIPO_MOV" in first_cajero.columns else first_cajero

        # Montos de tarjeta en First para este cajero
        montos_first = []
        for _, row in first_cajero.iterrows():
            deb = float(row.get("TT_TARJETA_DEBITO", 0) or 0)
            cred = float(row.get("TT_TARJETA_DE_CREDITO", 0) or 0)
            if deb > 0:
                montos_first.append({"monto": deb, "tipo": "debito", "nro_mov": row.get("NRO_MOV", ""), "cliente": str(row.get("CLIENTE_NOMBRE", "")).strip()})
            if cred > 0:
                montos_first.append({"monto": cred, "tipo": "credito", "nro_mov": row.get("NRO_MOV", ""), "cliente": str(row.get("CLIENTE_NOMBRE", "")).strip()})

        # Montos de tarjeta de la planilla del cajero
        # tarjetas_teorico ya es la suma; para el cruce ticket por ticket
        # necesitamos los montos individuales — están en reintegros o en el total
        # Por ahora usamos el total para validar suma
        montos_planilla_total = p.get("tarjetas_teorico", 0)
        first_total = sum(m["monto"] for m in montos_first)

        sin_match, match_exacto, match_aprox = _match_montos(montos_first, [])

        resultado[cajero] = {
            "first_total": first_total,
            "planilla_total": montos_planilla_total,
            "diferencia": first_total - montos_planilla_total,
            "tickets_first": montos_first,
            "sin_match_first": sin_match,
        }

    return resultado


def _cruzar_efectivo(planillas: list, first_df: pd.DataFrame) -> dict:
    """
    Cruza efectivo ticket por ticket, cajero por cajero.
    Planillas vs First (TT_EFECTIVO).
    """
    resultado = {}

    for p in planillas:
        cajero = p["nombre"]
        if "NOM_USUARIO" not in first_df.columns:
            continue
        first_cajero = first_df[
            first_df["NOM_USUARIO"].str.strip().str.upper() == cajero
        ].copy()
        first_cajero = first_cajero[
            first_cajero.get("NOM_TIPO_MOV", pd.Series(dtype=str)).str.contains("Factura", na=False, case=False)
        ] if "NOM_TIPO_MOV" in first_cajero.columns else first_cajero

        montos_first = []
        for _, row in first_cajero.iterrows():
            ef = float(row.get("TT_EFECTIVO", 0) or 0)
            if ef > 0:
                montos_first.append({
                    "monto": ef,
                    "nro_mov": row.get("NRO_MOV", ""),
                    "cliente": str(row.get("CLIENTE_NOMBRE", "")).strip()
                })

        first_total = sum(m["monto"] for m in montos_first)
        planilla_total = p.get("efectivo_teorico", 0)

        resultado[cajero] = {
            "first_total": first_total,
            "planilla_total": planilla_total,
            "diferencia": first_total - planilla_total,
            "tickets_first": montos_first,
        }

    return resultado


def _cruzar_transferencias(mp_transferencias: list, first_df: pd.DataFrame) -> dict:
    """
    Cruza transferencias MP vs First (TT_MERCADO_PAGO_TRANSFERENCIA).
    Match exacto primero, luego por tolerancia.
    """
    # Montos del First por transferencia MP
    col = "TT_MERCADO_PAGO_TRANSFERENCIA"
    if col not in first_df.columns:
        return {"error": "Columna TT_MERCADO_PAGO_TRANSFERENCIA no encontrada en First"}

    facturas = first_df[
        first_df.get("NOM_TIPO_MOV", pd.Series(dtype=str)).str.contains("Factura", na=False, case=False)
    ] if "NOM_TIPO_MOV" in first_df.columns else first_df

    montos_first = []
    for _, row in facturas.iterrows():
        mp = float(row.get(col, 0) or 0)
        if mp > 0:
            montos_first.append({
                "monto": mp,
                "cajero": str(row.get("NOM_USUARIO", "")).strip().upper(),
                "nro_mov": row.get("NRO_MOV", ""),
                "cliente": str(row.get("CLIENTE_NOMBRE", "")).strip()
            })

    montos_mp = [{"monto": t["monto"], "nombre": t["nombre"]} for t in mp_transferencias]

    sin_match_first, sin_match_mp, matches = _match_bidireccional(montos_first, montos_mp)

    total_first = sum(m["monto"] for m in montos_first)
    total_mp = sum(m["monto"] for m in montos_mp)

    return {
        "total_first": total_first,
        "total_mp": total_mp,
        "diferencia": total_mp - total_first,
        "matches": matches,
        "sin_match_first": sin_match_first,  # En First pero no en MP
        "sin_match_mp": sin_match_mp,         # En MP pero no en First
    }


def _match_montos(montos_a: list, montos_b: list) -> tuple:
    """Match exacto + aproximado. Devuelve (sin_match_a, match_exacto, match_aprox)."""
    usados_b = [False] * len(montos_b)
    match_exacto = []
    match_aprox = []
    sin_match_a = []

    for item_a in montos_a:
        monto_a = item_a["monto"]
        encontrado = False

        # Paso 1: match exacto
        for j, item_b in enumerate(montos_b):
            if not usados_b[j] and abs(item_b["monto"] - monto_a) < 0.01:
                match_exacto.append({"a": item_a, "b": item_b})
                usados_b[j] = True
                encontrado = True
                break

        if not encontrado:
            # Paso 2: match aproximado (TOLERANCIA_PCT)
            for j, item_b in enumerate(montos_b):
                if not usados_b[j]:
                    pct = abs(item_b["monto"] - monto_a) / monto_a if monto_a else 0
                    if pct <= TOLERANCIA_PCT:
                        match_aprox.append({"a": item_a, "b": item_b, "diff_pct": round(pct * 100, 3)})
                        usados_b[j] = True
                        encontrado = True
                        break

        if not encontrado:
            sin_match_a.append(item_a)

    return sin_match_a, match_exacto, match_aprox


def _match_bidireccional(montos_first: list, montos_mp: list) -> tuple:
    """
    Match bidireccional con 3 pasos:
    1. Match exacto 1-a-1
    2. Match aproximado 1-a-1 (tolerancia %)
    3. Match 1-a-muchos: un pago MP puede cubrir varios tickets del First
       (mismo cajero/cliente, montos consecutivos que suman igual)
    Devuelve (sin_match_first, sin_match_mp, matches).
    """
    usados_mp = [False] * len(montos_mp)
    usados_first = [False] * len(montos_first)
    matches = []

    # ── Paso 1: match exacto 1-a-1 ──────────────────────────────────────────
    for i, item_f in enumerate(montos_first):
        mf = item_f["monto"]
        for j, item_mp in enumerate(montos_mp):
            if not usados_mp[j] and not usados_first[i]:
                if abs(item_mp["monto"] - mf) < 0.01:
                    matches.append({"first": [item_f], "mp": item_mp, "tipo": "exacto"})
                    usados_first[i] = True
                    usados_mp[j] = True
                    break

    # ── Paso 2: match aproximado 1-a-1 ──────────────────────────────────────
    for i, item_f in enumerate(montos_first):
        if usados_first[i]:
            continue
        mf = item_f["monto"]
        for j, item_mp in enumerate(montos_mp):
            if usados_mp[j]:
                continue
            pct = abs(item_mp["monto"] - mf) / mf if mf else 0
            if pct <= TOLERANCIA_PCT:
                matches.append({"first": [item_f], "mp": item_mp, "tipo": "aproximado", "diff_pct": round(pct * 100, 3)})
                usados_first[i] = True
                usados_mp[j] = True
                break

    # ── Paso 3: match 1-a-muchos ─────────────────────────────────────────────
    # Para cada pago MP sin match, buscamos combinaciones de tickets First
    # que sumen igual (exacto o aproximado), priorizando mismo cajero/cliente
    pendientes_first_idx = [i for i, used in enumerate(usados_first) if not used]
    pendientes_mp_idx = [j for j, used in enumerate(usados_mp) if not used]

    for j in pendientes_mp_idx:
        if usados_mp[j]:
            continue
        monto_mp = montos_mp[j]["monto"]
        nombre_mp = montos_mp[j].get("nombre", "").lower()

        # Candidatos: tickets First sin usar
        candidatos = [
            (i, montos_first[i]) for i in pendientes_first_idx
            if not usados_first[i]
        ]

        # Ordenar: primero los que tienen cliente parecido al nombre MP
        def score(item):
            cliente = item[1].get("cliente", "").lower()
            # Coincidencia parcial de nombre
            partes_mp = nombre_mp.split()
            coincidencias = sum(1 for p in partes_mp if p in cliente)
            return -coincidencias  # negativo para orden desc
        candidatos_sorted = sorted(candidatos, key=score)

        # Buscar combinación que sume el monto MP (máximo 8 tickets)
        combinacion = _buscar_combinacion(
            [c[1]["monto"] for c in candidatos_sorted],
            monto_mp,
            max_items=8,
            tolerancia=TOLERANCIA_PCT
        )

        if combinacion is not None:
            idxs_candidatos = [candidatos_sorted[k][0] for k in combinacion]
            items_first = [montos_first[i] for i in idxs_candidatos]
            suma = sum(f["monto"] for f in items_first)
            pct = abs(suma - monto_mp) / monto_mp if monto_mp else 0
            matches.append({
                "first": items_first,
                "mp": montos_mp[j],
                "tipo": "agrupado" + ("_aprox" if pct > 0.001 else ""),
                "diff_pct": round(pct * 100, 3) if pct > 0.001 else None,
                "n_tickets": len(items_first),
            })
            for i in idxs_candidatos:
                usados_first[i] = True
            usados_mp[j] = True

    sin_match_first = [m for i, m in enumerate(montos_first) if not usados_first[i]]
    sin_match_mp = [m for j, m in enumerate(montos_mp) if not usados_mp[j]]

    return sin_match_first, sin_match_mp, matches


def _buscar_combinacion(montos: list, objetivo: float, max_items: int = 8, tolerancia: float = 0.02) -> Optional[list]:
    """
    Busca una combinación de índices de montos[] cuya suma sea igual a objetivo
    (con tolerancia). Usa backtracking limitado a max_items para eficiencia.
    Devuelve lista de índices o None si no encuentra.
    """
    from itertools import combinations
    n = min(len(montos), 20)  # limitar a 20 candidatos para performance
    montos_limitados = montos[:n]

    for size in range(2, min(max_items + 1, n + 1)):
        for combo in combinations(range(n), size):
            suma = sum(montos_limitados[k] for k in combo)
            pct = abs(suma - objetivo) / objetivo if objetivo else 0
            if pct <= tolerancia:
                return list(combo)
    return None


def _identificar_posibles_causas(monto: float, cajero: str, medio: str, first_df: pd.DataFrame) -> list:
    """
    Para un ticket sin match, sugiere posibles causas comparando con otros medios de pago.
    """
    causas = []
    if "NOM_USUARIO" not in first_df.columns:
        return ["No se puede analizar: falta columna NOM_USUARIO"]

    first_cajero = first_df[first_df["NOM_USUARIO"].str.strip().str.upper() == cajero.upper()]

    # ¿Aparece ese monto en otro medio de pago?
    otros_medios = {
        "TT_EFECTIVO": "efectivo",
        "TT_TARJETA_DEBITO": "tarjeta débito",
        "TT_TARJETA_DE_CREDITO": "tarjeta crédito",
        "TT_MERCADO_PAGO_TRANSFERENCIA": "transferencia MP",
    }
    for col, label in otros_medios.items():
        if label == medio:
            continue
        if col not in first_cajero.columns:
            continue
        for _, row in first_cajero.iterrows():
            val = float(row.get(col, 0) or 0)
            if abs(val - monto) < 0.01:
                causas.append(f"Cobrado en {label} pero registrado en {medio}")
                break

    if not causas:
        causas.append("Ticket no anulado en sistema")
        causas.append("Pedido no entregado / diferido para el día siguiente")

    return causas


# ── ENDPOINT PRINCIPAL ────────────────────────────────────────────────────────

@router.post("/procesar")
async def procesar_caja(
    planilla_aaron: UploadFile = File(None),
    planilla_christian: UploadFile = File(None),
    planilla_aylen: UploadFile = File(None),
    planilla_lautaro: UploadFile = File(None),
    planilla_ignacio: UploadFile = File(None),
    reporte_first: UploadFile = File(...),
    reporte_mp: UploadFile = File(...),
    efectivo_aaron: float = Form(0),
    efectivo_christian: float = Form(0),
    efectivo_aylen: float = Form(0),
    efectivo_lautaro: float = Form(0),
    efectivo_ignacio: float = Form(0),
    cierre_posnet: float = Form(...),
    tkt_anulado_otro_dia: float = Form(0),
    pago_nc_papel: float = Form(0),
):
    try:
        # ── Leer planillas ───────────────────────────────────────────────────
        planillas_input = {
            "AARON": planilla_aaron,
            "CHRISTIAN": planilla_christian,
            "AYLEN": planilla_aylen,
            "LAUTARO": planilla_lautaro,
            "IGNACIO": planilla_ignacio,
        }
        efectivo_por_cajero = {
            "AARON": efectivo_aaron,
            "CHRISTIAN": efectivo_christian,
            "AYLEN": efectivo_aylen,
            "LAUTARO": efectivo_lautaro,
            "IGNACIO": efectivo_ignacio,
        }

        planillas_data = []
        for nombre, upload in planillas_input.items():
            if upload is not None:
                file_bytes = await upload.read()
                datos = extraer_datos_planilla_cajero(file_bytes, upload.filename or f"{nombre}.xlsx")
                # Override nombre con el key conocido
                datos["nombre"] = nombre
                planillas_data.append(datos)

        if not planillas_data:
            raise HTTPException(status_code=400, detail="No se subió ninguna planilla de cajero")

        # ── Leer First ───────────────────────────────────────────────────────
        first_bytes = await reporte_first.read()
        first_df = leer_reporte_first(first_bytes)

        # ── Leer MP ──────────────────────────────────────────────────────────
        mp_bytes = await reporte_mp.read()
        mp_transferencias = leer_reporte_mp(mp_bytes)

        # ── Calcular ─────────────────────────────────────────────────────────
        resultado = calcular_control_caja(
            planillas=planillas_data,
            first_df=first_df,
            mp_transferencias=mp_transferencias,
            efectivo_por_cajero={k: v for k, v in efectivo_por_cajero.items()
                                  if any(p["nombre"] == k for p in planillas_data)},
            cierre_posnet=cierre_posnet,
            tkt_anulado_otro_dia=tkt_anulado_otro_dia,
            pago_nc_papel=pago_nc_papel,
        )

        # ── Enriquecer tickets sin match con posibles causas ─────────────────
        for medio_key, cruce in resultado["cruces"].items():
            if medio_key == "transferencias":
                for ticket in cruce.get("sin_match_first", []):
                    ticket["posibles_causas"] = _identificar_posibles_causas(
                        ticket["monto"], ticket.get("cajero", ""), "transferencia MP", first_df
                    )
                for ticket in cruce.get("sin_match_mp", []):
                    ticket["posibles_causas"] = ["Transferencia no registrada en First / ingresada en otro medio"]
            elif medio_key == "tarjetas":
                for cajero, data in cruce.items():
                    if isinstance(data, dict):
                        for ticket in data.get("sin_match_first", []):
                            ticket["posibles_causas"] = _identificar_posibles_causas(
                                ticket["monto"], cajero, f"tarjeta {ticket.get('tipo','')}", first_df
                            )

        return JSONResponse(content=resultado)

    except HTTPException:
        raise
    except Exception as e:
        raise HTTPException(status_code=500, detail=f"Error procesando caja: {str(e)}")


@router.get("/cajeros")
def get_cajeros():
    """Devuelve la lista de cajeros configurados."""
    return {"cajeros": CAJEROS_CONOCIDOS}
