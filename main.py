from fastapi import FastAPI, UploadFile, File, Form
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import StreamingResponse
import anthropic
import io
import os
import pandas as pd
from datetime import datetime
from collections import defaultdict
from openpyxl import Workbook
from openpyxl.styles import Font, Border, Side, Alignment, PatternFill
from openpyxl.worksheet.table import Table, TableStyleInfo
import base64
import json

app = FastAPI()

app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

client = anthropic.Anthropic(api_key=os.environ.get("ANTHROPIC_API_KEY"))

DARK_BLUE='1F3864'; MID_BLUE='2E5FA3'; LIGHT_BLUE='D6E4F0'; WHITE='FFFFFF'; GREEN='1E8449'; ORANGE='C0392B'

def parse_date(s):
    if not s: return datetime.now()
    for fmt in ('%d/%m/%Y','%Y-%m-%d','%d-%m-%Y'):
        try: return datetime.strptime(s.strip(), fmt)
        except: pass
    return datetime.now()

def days_diff(d1, d2):
    return (d2 - d1).days

def ask_claude(content, system):
    msg = client.messages.create(
        model="claude-sonnet-4-20250514",
        max_tokens=2000,
        system=system,
        messages=[{"role": "user", "content": content}]
    )
    return msg.content[0].text

def pdf_to_content(pdf_bytes, label):
    b64 = base64.standard_b64encode(pdf_bytes).decode()
    return [
        {"type": "document", "source": {"type": "base64", "media_type": "application/pdf", "data": b64}},
        {"type": "text", "text": f"Documento: {label}"}
    ]

def parse_json(text):
    text = text.strip()
    start = text.find('{')
    end = text.rfind('}') + 1
    if start >= 0 and end > start:
        return json.loads(text[start:end])
    raise ValueError(f"No JSON found in: {text[:200]}")

def xls_to_text(file_bytes, filename):
    import tempfile
    # Detectar si es XML/HTML disfrazado de XLS (común en sistemas de obras sociales)
    sample = file_bytes[:20]
    is_xml = (sample.startswith(b'\xff\xfe') or sample.startswith(b'<?xml') or
              sample.startswith(b'<') or b'<html' in sample[:100].lower() or
              b'<HTML' in sample[:100])
    if is_xml:
        # Decodificar como XML/HTML y extraer texto
        try:
            text = file_bytes.decode('utf-16')
        except Exception:
            try:
                text = file_bytes.decode('utf-8', errors='replace')
            except Exception:
                text = file_bytes.decode('latin-1', errors='replace')
        # Usar pandas para leer HTML
        import io as _io
        try:
            tables = pd.read_html(_io.StringIO(text))
            if tables:
                return '\n'.join(df.to_csv(index=False) for df in tables)
        except Exception:
            pass
        # Fallback: devolver texto plano sin tags
        import re
        clean = re.sub(r'<[^>]+>', ' ', text)
        clean = re.sub(r'\s+', ' ', clean)
        return clean
    suffix = '.xlsx' if filename.endswith('.xlsx') else '.xls'
    with tempfile.NamedTemporaryFile(suffix=suffix, delete=False) as tmp:
        tmp.write(file_bytes)
        tmp_path = tmp.name
    try:
        engine = "xlrd" if suffix == ".xls" else "openpyxl"
        df = pd.read_excel(tmp_path, header=None, engine=engine)
        return df.to_csv(index=False)
    finally:
        os.unlink(tmp_path)

SYSTEM_JSON = 'CRÍTICO: Respondé ÚNICAMENTE con el objeto JSON solicitado. Sin texto, sin explicaciones, sin markdown. Empezá con { y terminá con }.'

# ── EXCEL BUILDERS ────────────────────────────────────────────────────────────

def c(ws, coord, val, bold=False, size=10, color='000000', fill=None, halign='left', num_fmt=None):
    cell = ws[coord]
    cell.value = val
    cell.font = Font(bold=bold, size=size, color=color)
    cell.alignment = Alignment(horizontal=halign, vertical='center')
    if fill: cell.fill = PatternFill('solid', fgColor=fill)
    if num_fmt: cell.number_format = num_fmt

def n(ws, coord, val):
    c(ws, coord, val, halign='right', num_fmt='#,##0.00')

def ni(ws, coord, val):
    """Número entero sin decimales"""
    c(ws, coord, round(val) if val else 0, halign='right', num_fmt='#,##0')

def d(ws, coord, val):
    ws[coord].value = val
    ws[coord].number_format = 'DD/MM/YYYY'
    ws[coord].font = Font(size=10)
    ws[coord].alignment = Alignment(horizontal='right', vertical='center')

def box(ws, min_r, min_c, max_r, max_c):
    s = Side(style='medium', color=DARK_BLUE)
    for row in ws.iter_rows(min_row=min_r, max_row=max_r, min_col=min_c, max_col=max_c):
        for cell in row:
            t = s if cell.row == min_r else None
            b = s if cell.row == max_r else None
            l = s if cell.column == min_c else None
            r = s if cell.column == max_c else None
            cell.border = Border(top=t, bottom=b, left=l, right=r)

def setup_ws(ws):
    ws.sheet_view.showGridLines = False
    for col, w in {'A':2,'B':20,'C':16,'D':3,'E':22,'F':16,'G':3,'H':22,'I':19,'J':3,'K':16,'L':14}.items():
        ws.column_dimensions[col].width = w
    for r, h in {1:12,2:18,3:26,4:12,5:18,6:26,7:12,8:18,9:26,10:12}.items():
        ws.row_dimensions[r].height = h
    for r in range(11, 50):
        ws.row_dimensions[r].height = 16

def header_bg(ws):
    for r in range(2, 4):
        for col in range(2, 13):
            ws.cell(r, col).fill = PatternFill('solid', fgColor=DARK_BLUE)
    for col in [4, 10, 12]:
        for r in [2, 3]:
            ws.cell(r, col).fill = PatternFill('solid', fgColor=DARK_BLUE)

def add_resumen_table(ws2, headers, data, table_name, ref, style):
    for i, h in enumerate(headers):
        ws2.cell(1, i+1, h)
    for i, v in enumerate(data):
        cell = ws2.cell(2, i+1)
        cell.value = v
        if isinstance(v, float) and abs(v) < 10:
            cell.number_format = '0.00%'
        else:
            cell.number_format = '#,##0.00'
    tbl = Table(displayName=table_name, ref=ref)
    tbl.tableStyleInfo = style
    ws2.add_table(tbl)

# ── PAMI ──────────────────────────────────────────────────────────────────────

def build_pami_excel(data, q, mes, anio):
    car = data['caratula']; opf = data['opf']; pre = data['pre']; pago = data['pago']; nr = data['nr']

    fecha_pres = parse_date(car['fecha_cierre'])
    dias_ant = days_diff(fecha_pres, parse_date(opf['fecha_opf']))
    dias_liq = days_diff(fecha_pres, parse_date(pago['fecha_pago']))
    dias_nr = days_diff(fecha_pres, parse_date(nr.get('fecha_nr', car['fecha_cierre'])))
    dias_efsa = days_diff(fecha_pres, parse_date(nr.get('fecha_efsa', car['fecha_cierre'])))

    CCF=nr.get('nr_ccf',0); CCFD=nr.get('nr_ccfd',0); NAF=nr.get('nr_naf',0)
    NRFD=nr.get('nr_nrfd',0); EfSa=nr.get('nr_efsa',0)
    total_ccf_ccfd = CCF+CCFD; total_naf_nrfd = NAF+NRFD
    liq_final = pre['total_liquidacion'] - opf['efvo_pami']
    total_pagado = opf['efvo_pami'] + liq_final + total_ccf_ccfd + total_naf_nrfd + EfSa
    afiliado = car['total_pvp_pami'] - car['importe_bruto_convenio']
    total = afiliado + total_pagado
    deb_os = abs(pre['deb_cred_os']) if pre['deb_cred_os'] < 0 else 0
    cred_os = pre['deb_cred_os'] if pre['deb_cred_os'] > 0 else 0
    bonificaciones = abs(pre['bonif_ambulatorio']) + abs(pre['bonif_tiras']) + abs(pre['bonif_insulinas'])
    retenciones = abs(pre['ret_gtos_adm_cofa']) + abs(pre['retencion_colegio_art12']) + abs(pre['fdo_prest_colfarma'])
    notas_cred = abs(pre['nota_cred_ambulatorio']) + abs(pre['nota_cred_insulina']) + abs(pre['nota_cred_tiras'])
    diferencia_total = car['total_pvp'] - car['total_pvp_pami']
    pct70 = diferencia_total * 0.7; pct30 = diferencia_total * 0.3
    dif_nr = total_naf_nrfd - notas_cred
    dif_efvo = EfSa - abs(pre['efectivo_drogueria'])
    dif_ccf = total_ccf_ccfd - pct70
    dias_prom = (opf['efvo_pami']*dias_ant + liq_final*dias_liq + (total_ccf_ccfd+total_naf_nrfd)*dias_nr + EfSa*dias_efsa) / total_pagado
    periodo = f'{q}/{mes}/{anio}'

    wb = Workbook()
    ws = wb.active; ws.title = 'Reporte'
    setup_ws(ws); header_bg(ws)

    ws.merge_cells('B2:C3'); c(ws,'B2','PAMI',bold=True,size=28,color=WHITE,fill=DARK_BLUE,halign='center')
    ws.merge_cells('E2:F3'); c(ws,'E2',periodo,bold=True,size=20,color=WHITE,fill=DARK_BLUE,halign='center')
    c(ws,'H2','RECETAS',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'H3',car['nro_recetas'],bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center',num_fmt='#,##0')
    c(ws,'I2','FECHA DE PRESENTACION',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'I3',fecha_pres.strftime('%d/%m/%Y'),bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center')
    c(ws,'K2','DÍAS PROM.',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'K3',round(dias_prom,1),bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center',num_fmt='#,##0.0')

    ws.merge_cells('B5:C5'); c(ws,'B5','TOTAL PVP',size=9,color=WHITE,fill=MID_BLUE,halign='center')
    ws.merge_cells('B6:C6'); ni(ws,'B6',car['total_pvp']); ws['B6'].font=Font(bold=True,size=13,color=WHITE); ws['B6'].fill=PatternFill('solid',fgColor=MID_BLUE); ws['B6'].alignment=Alignment(horizontal='center',vertical='center')
    ws.merge_cells('E5:F5'); c(ws,'E5','PVP PAMI',size=9,color=WHITE,fill=MID_BLUE,halign='center')
    ws.merge_cells('E6:F6'); ni(ws,'E6',car['total_pvp_pami']); ws['E6'].font=Font(bold=True,size=13,color=WHITE); ws['E6'].fill=PatternFill('solid',fgColor=MID_BLUE); ws['E6'].alignment=Alignment(horizontal='center',vertical='center')
    ws.merge_cells('H5:I5'); c(ws,'H5','AFILIADO',size=9,color=WHITE,fill=MID_BLUE,halign='center')
    ws.merge_cells('H6:I6'); ni(ws,'H6',afiliado); ws['H6'].font=Font(bold=True,size=13,color=WHITE); ws['H6'].fill=PatternFill('solid',fgColor=MID_BLUE); ws['H6'].alignment=Alignment(horizontal='center',vertical='center')
    ws.merge_cells('K5:L5'); c(ws,'K5','TOTAL PAGADO PAMI',size=9,color=WHITE,fill=GREEN,halign='center')
    ws.merge_cells('K6:L6'); ni(ws,'K6',total_pagado); ws['K6'].font=Font(bold=True,size=13,color=WHITE); ws['K6'].fill=PatternFill('solid',fgColor=GREEN); ws['K6'].alignment=Alignment(horizontal='center',vertical='center')

    for col,col_dias,lbl,dias,val in [('B','C','ANTICIPO',dias_ant,opf['efvo_pami']),('E','F','LIQ. FINAL',dias_liq,liq_final),('H','I','NOTAS RECUP.',dias_nr,total_ccf_ccfd+total_naf_nrfd),('K','L','EFVO. DROG.',dias_efsa,EfSa)]:
        c(ws,f'{col}8',lbl,size=9,color=WHITE,fill=MID_BLUE,halign='center')
        c(ws,f'{col_dias}8','DIAS DE PAGO',size=9,color=WHITE,fill=MID_BLUE,halign='center')
        n(ws,f'{col}9',val); ws[f'{col}9'].font=Font(bold=True,size=13,color=WHITE); ws[f'{col}9'].fill=PatternFill('solid',fgColor=MID_BLUE); ws[f'{col}9'].alignment=Alignment(horizontal='center',vertical='center')
        c(ws,f'{col_dias}9',dias,bold=True,size=13,color=WHITE,fill=MID_BLUE,halign='center')

    ws.merge_cells('B11:C11'); c(ws,'B11','PAGOS A FARMACIA',bold=True,size=11,halign='center')
    ws.merge_cells('E11:F11'); c(ws,'E11','DESCUENTOS',bold=True,size=11,halign='center')
    ws.merge_cells('H11:L11'); c(ws,'H11','PAGOS A DROGUERIA',bold=True,size=11,halign='center')

    ws.merge_cells('B12:C12'); c(ws,'B12','ANTICIPO (OPF)',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'B13','TOTAL',bold=True); n(ws,'C13',opf['efvo_pami'])
    c(ws,'B14','Fecha pago'); d(ws,'C14',parse_date(opf['fecha_opf']))
    c(ws,'B15','Comprobante'); ws['C15'].value=opf['nro_comprobante_opf']; ws['C15'].alignment=Alignment(horizontal='right',vertical='center'); ws['C15'].font=Font(size=10)
    ws.merge_cells('B16:C16'); c(ws,'B16','LIQUIDACIÓN',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'B17','Bruto a pagar antes imp.:'); n(ws,'C17',pre['total_liquidacion'])
    c(ws,'B18','TOTAL',bold=True); n(ws,'C18',liq_final)
    c(ws,'B19','Fecha pago'); d(ws,'C19',parse_date(pago['fecha_pago']))
    c(ws,'B20','Comprobante'); ws['C20'].value=pago['nro_comprobante_pago']; ws['C20'].alignment=Alignment(horizontal='right',vertical='center'); ws['C20'].font=Font(size=10)
    box(ws,11,2,20,3)

    ws.merge_cells('E12:F12'); c(ws,'E12','BONIFICACIONES',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E13','Ambulatorio:'); n(ws,'F13',abs(pre['bonif_ambulatorio']))
    c(ws,'E14','Tiras:'); n(ws,'F14',abs(pre['bonif_tiras']))
    c(ws,'E15','Insulinas:'); n(ws,'F15',abs(pre['bonif_insulinas']))
    c(ws,'E16','TOTAL',bold=True); n(ws,'F16',bonificaciones)
    ws.merge_cells('E17:F17'); c(ws,'E17','RETENCIONES',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E18','Gastos Adm. COFA:'); n(ws,'F18',abs(pre['ret_gtos_adm_cofa']))
    c(ws,'E19','Colegio Art. 12 SU:'); n(ws,'F19',abs(pre['retencion_colegio_art12']))
    c(ws,'E20','Fdo Prest. COLFARMA:'); n(ws,'F20',abs(pre['fdo_prest_colfarma']))
    c(ws,'E21','TOTAL',bold=True); n(ws,'F21',retenciones)
    ws.merge_cells('E22:F22'); c(ws,'E22','DÉB. / CRÉD. OS',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E23','Débito OS:'); n(ws,'F23',deb_os)
    c(ws,'E24','Crédito OS:'); n(ws,'F24',cred_os)
    box(ws,11,5,24,6)

    ws.merge_cells('H12:L12'); c(ws,'H12','NOTAS DE CRÉDITO',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'I13','Monto Calculado',bold=True,halign='center'); c(ws,'K13','Monto según NR',bold=True,halign='center'); c(ws,'L13','Diferencia',bold=True,halign='center')
    c(ws,'H14','Ambulatorio:'); n(ws,'I14',abs(pre['nota_cred_ambulatorio']))
    c(ws,'H15','Tiras:'); n(ws,'I15',abs(pre['nota_cred_tiras']))
    c(ws,'H16','Insulinas:'); n(ws,'I16',abs(pre['nota_cred_insulina']))
    c(ws,'H17','TOTAL',bold=True); n(ws,'I17',notas_cred); n(ws,'K17',total_naf_nrfd); n(ws,'L17',dif_nr)
    ws.merge_cells('H18:L18'); c(ws,'H18','70% / 30%',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'I19','Monto Calculado',bold=True,halign='center'); c(ws,'K19','Monto según NR',bold=True,halign='center'); c(ws,'L19','Diferencia',bold=True,halign='center')
    c(ws,'H20','70% CCF/CCFD',bold=True); n(ws,'I20',pct70); n(ws,'K20',total_ccf_ccfd); n(ws,'L20',dif_ccf)
    c(ws,'H21','30% Pérdida'); n(ws,'I21',pct30)
    ws.merge_cells('H22:L22'); c(ws,'H22','EFECTIVO DROGUERÍA',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'I23','Monto Calculado',bold=True,halign='center'); c(ws,'K23','Monto según NR',bold=True,halign='center'); c(ws,'L23','Diferencia',bold=True,halign='center')
    c(ws,'H24','TOTAL',bold=True); n(ws,'I24',abs(pre['efectivo_drogueria'])); n(ws,'K24',EfSa); n(ws,'L24',dif_efvo)
    box(ws,11,8,24,12)

    ws2 = wb.create_sheet('Resumen'); ws2.sheet_view.showGridLines = False
    style = TableStyleInfo(name='TableStyleMedium2',showFirstColumn=False,showLastColumn=False,showRowStripes=True,showColumnStripes=False)

    h1=['RECETAS','PVP','PVP PAMI','TOTAL','%PVP','AFILIADO','%TOTAL AFL','PAMI','%TOTAL PAMI','DIAS PROM. PAGO','DEBITOS','RETENCIONES','%PVP PAMI RET','BONIFICACIONES','%PVP PAMI BON']
    for i,h in enumerate(h1): ws2.cell(1,i+1,h)
    row2=[car['nro_recetas'],car['total_pvp'],car['total_pvp_pami'],total,total/car['total_pvp'],afiliado,afiliado/total,total_pagado,total_pagado/total,round(dias_prom,2),deb_os,retenciones,retenciones/car['total_pvp_pami'],bonificaciones,bonificaciones/car['total_pvp_pami']]
    for i,v in enumerate(row2):
        cell=ws2.cell(2,i+1); cell.value=v
        cell.number_format='0.00%' if isinstance(v,float) and abs(v)<10 else '#,##0.00'
    tbl1=Table(displayName='tbl_reporte',ref='A1:O2'); tbl1.tableStyleInfo=style; ws2.add_table(tbl1)

    h2=['ANTICIPO','%PAMI ANT','DIAS ANT','LIQ FINAL','%PAMI LIQ FINAL','DIAS LIQ FINAL','NOTAS DE RECUPERO','%PAMI NR','DIAS NR','EFVO DROG','%PAMI EFVO DROG','DIAS EFVO DROG']
    for i,h in enumerate(h2): ws2.cell(4,i+1,h)
    row5=[opf['efvo_pami'],opf['efvo_pami']/total_pagado,dias_ant,liq_final,liq_final/total_pagado,dias_liq,total_ccf_ccfd+total_naf_nrfd,(total_ccf_ccfd+total_naf_nrfd)/total_pagado,dias_nr,EfSa,EfSa/total_pagado,dias_efsa]
    for i,v in enumerate(row5):
        cell=ws2.cell(5,i+1); cell.value=v
        cell.number_format='0.00%' if isinstance(v,float) and abs(v)<10 else '#,##0.00'
    tbl2=Table(displayName='tbl_desglose',ref='A4:L5'); tbl2.tableStyleInfo=style; ws2.add_table(tbl2)

    for i,h in enumerate(['Diferencias NR','Diferencias EFVO DROG','Diferencias CCF']): ws2.cell(7,i+1,h)
    for i,v in enumerate([dif_nr,dif_efvo,dif_ccf]):
        cell=ws2.cell(8,i+1); cell.value=v; cell.number_format='#,##0.00'
    tbl3=Table(displayName='tbl_diferencias',ref='A7:C8'); tbl3.tableStyleInfo=style; ws2.add_table(tbl3)
    for col in 'ABCDEFGHIJKLMNO': ws2.column_dimensions[col].width=20

    buf = io.BytesIO(); wb.save(buf); buf.seek(0)
    return buf

# ── IOMA ──────────────────────────────────────────────────────────────────────

def build_ioma_excel(data, mes, anio):
    planes = data['planes']; opf = data['opf']; pre = data['pre']; pago = data['pago']; nr_data = data['nr']

    total_recetas = sum(p.get('recetas',0) for p in planes.values())
    total_importe100 = sum(p.get('importe100',0) for p in planes.values())
    total_ac = sum(p.get('ac_instituto',0) for p in planes.values())
    afiliado = total_importe100 - total_ac
    fecha_pres = parse_date(data['fecha_cierre'])

    total_nr=0; total_pond=0
    nr_por_fecha = nr_data.get('nr_por_fecha',[])
    for item in nr_por_fecha:
        m=item.get('monto',0); total_nr+=m
        total_pond+=m*days_diff(fecha_pres,parse_date(item.get('fecha','')))
    dias_nr_pond = total_pond/total_nr if total_nr else 0

    total_ing_brutos = opf['ing_brutos_anticipo'] + pago['ing_brutos_pago']
    liq_final = pre['total_liquidacion'] - opf['efvo_ioma'] - total_ing_brutos
    total_pagado = opf['efvo_ioma'] + liq_final + total_nr
    deb_os = abs(pre['deb_cred_os']) if pre['deb_cred_os']<0 else 0
    cred_os = pre['deb_cred_os'] if pre['deb_cred_os']>0 else 0
    ret_cofa = abs(pre['retencion_colegio_art12']) + abs(pre['fdo_prest_colfarma'])
    notas_cred = abs(pre['nrf_ant']) + abs(pre['nrf_def']) + abs(pre['nrf_directas'])
    dif_nr = total_nr - notas_cred
    dias_ant = days_diff(fecha_pres, parse_date(opf['fecha_opf']))
    dias_liq = days_diff(fecha_pres, parse_date(pago['fecha_pago']))
    dias_prom = (opf['efvo_ioma']*dias_ant + liq_final*dias_liq + total_nr*dias_nr_pond) / total_pagado
    periodo = f'{mes}/{anio}'
    planes_activos = {k:v for k,v in planes.items() if v.get('recetas',0)>0}

    wb = Workbook()
    ws = wb.active; ws.title = 'Reporte'
    setup_ws(ws); header_bg(ws)

    ws.merge_cells('B2:C3'); c(ws,'B2','IOMA',bold=True,size=28,color=WHITE,fill=DARK_BLUE,halign='center')
    ws.merge_cells('E2:F3'); c(ws,'E2',periodo,bold=True,size=20,color=WHITE,fill=DARK_BLUE,halign='center')
    c(ws,'H2','RECETAS',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'H3',total_recetas,bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center',num_fmt='#,##0')
    c(ws,'I2','FECHA DE PRESENTACION',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'I3',fecha_pres.strftime('%d/%m/%Y'),bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center')
    c(ws,'K2','DÍAS PROM.',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'K3',round(dias_prom,1),bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center',num_fmt='#,##0.0')

    for coord,label,val,clr in [('B5:C5','IMPORTE 100%',total_importe100,MID_BLUE),('E5:F5','A/C INSTITUTO',total_ac,MID_BLUE),('H5:I5','AFILIADO',afiliado,MID_BLUE),('K5:L5','TOTAL PAGADO IOMA',total_pagado,GREEN)]:
        ws.merge_cells(coord); start=coord.split(':')[0]; end=coord.split(':')[1]
        r=int(start[1]); c_letter=start[0]; end_letter=end[0]
        c(ws,f'{c_letter}{r}',label,size=9,color=WHITE,fill=clr,halign='center')
        coord6=f'{c_letter}{r+1}:{end_letter}{r+1}'; ws.merge_cells(coord6)
        ni(ws,f'{c_letter}{r+1}',val); ws[f'{c_letter}{r+1}'].font=Font(bold=True,size=13,color=WHITE); ws[f'{c_letter}{r+1}'].fill=PatternFill('solid',fgColor=clr); ws[f'{c_letter}{r+1}'].alignment=Alignment(horizontal='center',vertical='center')

    for col,col_dias,lbl,dias,val in [('B','C','ANTICIPO',dias_ant,opf['efvo_ioma']),('E','F','LIQ. FINAL',dias_liq,liq_final),('H','I','NOTAS RECUP.',round(dias_nr_pond,1),total_nr)]:
        c(ws,f'{col}8',lbl,size=9,color=WHITE,fill=MID_BLUE,halign='center')
        c(ws,f'{col_dias}8','DIAS DE PAGO',size=9,color=WHITE,fill=MID_BLUE,halign='center')
        n(ws,f'{col}9',val); ws[f'{col}9'].font=Font(bold=True,size=13,color=WHITE); ws[f'{col}9'].fill=PatternFill('solid',fgColor=MID_BLUE); ws[f'{col}9'].alignment=Alignment(horizontal='center',vertical='center')
        c(ws,f'{col_dias}9',dias,bold=True,size=13,color=WHITE,fill=MID_BLUE,halign='center')
    ws.merge_cells('K8:L8'); c(ws,'K8','ING. BRUTOS RETENIDOS',size=9,color=WHITE,fill=ORANGE,halign='center')
    ws.merge_cells('K9:L9'); ni(ws,'K9',total_ing_brutos); ws['K9'].font=Font(bold=True,size=13,color=WHITE); ws['K9'].fill=PatternFill('solid',fgColor=ORANGE); ws['K9'].alignment=Alignment(horizontal='center',vertical='center')

    ws.merge_cells('B11:C11'); c(ws,'B11','CARÁTULAS',bold=True,size=11,halign='center')
    ws.merge_cells('E11:F11'); c(ws,'E11','DESCUENTOS',bold=True,size=11,halign='center')
    ws.merge_cells('H11:L11'); c(ws,'H11','PAGOS A DROGUERIA',bold=True,size=11,halign='center')

    ws.merge_cells('B12:C12'); c(ws,'B12','COMPOSICIÓN POR PLAN',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'B13','PLAN',bold=True); c(ws,'C13','RECETAS',bold=True,halign='right')
    row=14
    for plan,datos in planes_activos.items():
        c(ws,f'B{row}',plan); ws[f'C{row}'].value=datos['recetas']; ws[f'C{row}'].alignment=Alignment(horizontal='right',vertical='center'); ws[f'C{row}'].font=Font(size=10); row+=1
    c(ws,f'B{row}','TOTAL',bold=True); ws[f'C{row}'].value=total_recetas; ws[f'C{row}'].alignment=Alignment(horizontal='right',vertical='center'); ws[f'C{row}'].font=Font(bold=True,size=10)
    row_end_car=row; box(ws,11,2,row_end_car,3)

    row+=1; ws.row_dimensions[row].height=8
    row_start_pagos=row+1
    ws.merge_cells(f'B{row_start_pagos}:C{row_start_pagos}'); c(ws,f'B{row_start_pagos}','PAGOS A FARMACIA',bold=True,size=11,halign='center'); row=row_start_pagos+1
    ws.merge_cells(f'B{row}:C{row}'); c(ws,f'B{row}','ANTICIPO (OPF)',bold=True,size=10,fill=LIGHT_BLUE,halign='center'); row+=1
    c(ws,f'B{row}','TOTAL',bold=True); n(ws,f'C{row}',opf['efvo_ioma']); row+=1
    c(ws,f'B{row}','Fecha pago'); d(ws,f'C{row}',parse_date(opf['fecha_opf'])); row+=1
    c(ws,f'B{row}','Comprobante'); ws[f'C{row}'].value=opf['nro_comprobante_opf']; ws[f'C{row}'].alignment=Alignment(horizontal='right',vertical='center'); ws[f'C{row}'].font=Font(size=10); row+=1
    ws.merge_cells(f'B{row}:C{row}'); c(ws,f'B{row}','LIQUIDACIÓN',bold=True,size=10,fill=LIGHT_BLUE,halign='center'); row+=1
    c(ws,f'B{row}','Bruto a pagar antes imp.:'); n(ws,f'C{row}',pre['total_liquidacion']); row+=1
    c(ws,f'B{row}','TOTAL',bold=True); n(ws,f'C{row}',liq_final); row+=1
    c(ws,f'B{row}','Fecha pago'); d(ws,f'C{row}',parse_date(pago['fecha_pago'])); row+=1
    c(ws,f'B{row}','Comprobante'); ws[f'C{row}'].value=pago['nro_comprobante_pago']; ws[f'C{row}'].alignment=Alignment(horizontal='right',vertical='center'); ws[f'C{row}'].font=Font(size=10)
    row_end_pagos=row; box(ws,row_start_pagos,2,row_end_pagos,3)

    ws.merge_cells('E12:F12'); c(ws,'E12','IMPUESTOS',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E13','Ing. Btos + Gcias Anticipo:'); n(ws,'F13',opf['ing_brutos_anticipo'])
    c(ws,'E14','Ing. Btos + Gcias Pago Final:'); n(ws,'F14',pago['ing_brutos_pago'])
    c(ws,'E15','TOTAL',bold=True); n(ws,'F15',total_ing_brutos)
    ws.merge_cells('E16:F16'); c(ws,'E16','BONIFICACIONES',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E17','TOTAL',bold=True); n(ws,'F17',abs(pre['bonificaciones']))
    ws.merge_cells('E18:F18'); c(ws,'E18','RETENCIONES',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E19','Colegio Art. 12 SU:'); n(ws,'F19',abs(pre['retencion_colegio_art12']))
    c(ws,'E20','Fdo Prest. COLFARMA:'); n(ws,'F20',abs(pre['fdo_prest_colfarma']))
    c(ws,'E21','TOTAL',bold=True); n(ws,'F21',ret_cofa)
    ws.merge_cells('E22:F22'); c(ws,'E22','DÉB. / CRÉD. OS',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E23','Débito OS:'); n(ws,'F23',deb_os); c(ws,'E24','Crédito OS:'); n(ws,'F24',cred_os)
    box(ws,11,5,24,6)

    ws.merge_cells('H12:L12'); c(ws,'H12','NOTAS DE CRÉDITO',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'I13','Monto Calculado',bold=True,halign='center'); c(ws,'K13','Monto según NR',bold=True,halign='center'); c(ws,'L13','Diferencia',bold=True,halign='center')
    c(ws,'H14','NRF Anticipo:'); n(ws,'I14',abs(pre['nrf_ant']))
    c(ws,'H15','NRF Definitivo:'); n(ws,'I15',abs(pre['nrf_def']))
    c(ws,'H16','NRF Directas:'); n(ws,'I16',abs(pre['nrf_directas']))
    c(ws,'H17','TOTAL',bold=True); n(ws,'I17',notas_cred); n(ws,'K17',total_nr); n(ws,'L17',dif_nr)
    nr_row=19
    ws.merge_cells(f'H{nr_row}:L{nr_row}'); c(ws,f'H{nr_row}','DESGLOSE DE PAGOS',bold=True,size=10,fill=LIGHT_BLUE,halign='center'); nr_row+=1
    c(ws,f'H{nr_row}','Fecha',bold=True,halign='center'); c(ws,f'I{nr_row}','Monto',bold=True,halign='center'); c(ws,f'K{nr_row}','Días',bold=True,halign='center'); nr_row+=1
    for item in sorted(nr_por_fecha, key=lambda x: x.get('fecha','')):
        df=days_diff(fecha_pres,parse_date(item.get('fecha',''))); d(ws,f'H{nr_row}',parse_date(item.get('fecha',''))); n(ws,f'I{nr_row}',item.get('monto',0))
        ws[f'K{nr_row}'].value=df; ws[f'K{nr_row}'].alignment=Alignment(horizontal='right',vertical='center'); ws[f'K{nr_row}'].font=Font(size=10); nr_row+=1
    c(ws,f'H{nr_row}','DÍAS PROM.',bold=True); ws[f'K{nr_row}'].value=round(dias_nr_pond,1); ws[f'K{nr_row}'].alignment=Alignment(horizontal='right',vertical='center'); ws[f'K{nr_row}'].font=Font(bold=True,size=10)
    box(ws,11,8,nr_row,12)

    ws2=wb.create_sheet('Resumen'); ws2.sheet_view.showGridLines=False
    style=TableStyleInfo(name='TableStyleMedium2',showFirstColumn=False,showLastColumn=False,showRowStripes=True,showColumnStripes=False)
    h1=['RECETAS','IMPORTE 100%','A/C INSTITUTO','AFILIADO','%AFL','IOMA','%IOMA','DIAS PROM. PAGO','DEBITOS','RETENCIONES COFA','%RET','BONIFICACIONES','%BON','ING BRUTOS RETENIDOS']
    for i,h in enumerate(h1): ws2.cell(1,i+1,h)
    row2=[total_recetas,total_importe100,total_ac,afiliado,afiliado/total_importe100 if total_importe100 else 0,total_pagado,total_pagado/total_ac if total_ac else 0,round(dias_prom,2),deb_os,ret_cofa,ret_cofa/total_ac if total_ac else 0,abs(pre['bonificaciones']),abs(pre['bonificaciones'])/total_ac if total_ac else 0,total_ing_brutos]
    for i,v in enumerate(row2):
        cell=ws2.cell(2,i+1); cell.value=v; cell.number_format='0.00%' if isinstance(v,float) and abs(v)<10 else '#,##0.00'
    tbl1=Table(displayName='tbl_reporte',ref='A1:N2'); tbl1.tableStyleInfo=style; ws2.add_table(tbl1)
    h2=['ANTICIPO','%IOMA ANT','DIAS ANT','LIQ FINAL','%IOMA LIQ FINAL','DIAS LIQ FINAL','NOTAS DE RECUPERO','%IOMA NR','DIAS NR']
    for i,h in enumerate(h2): ws2.cell(4,i+1,h)
    row5=[opf['efvo_ioma'],opf['efvo_ioma']/total_pagado if total_pagado else 0,dias_ant,liq_final,liq_final/total_pagado if total_pagado else 0,dias_liq,total_nr,total_nr/total_pagado if total_pagado else 0,round(dias_nr_pond,2)]
    for i,v in enumerate(row5):
        cell=ws2.cell(5,i+1); cell.value=v; cell.number_format='0.00%' if isinstance(v,float) and abs(v)<10 else '#,##0.00'
    tbl2=Table(displayName='tbl_desglose',ref='A4:I5'); tbl2.tableStyleInfo=style; ws2.add_table(tbl2)
    ws2.cell(7,1,'Diferencias NR'); cell=ws2.cell(8,1); cell.value=dif_nr; cell.number_format='#,##0.00'
    tbl3=Table(displayName='tbl_diferencias',ref='A7:A8'); tbl3.tableStyleInfo=style; ws2.add_table(tbl3)
    for col in 'ABCDEFGHIJKLMN': ws2.column_dimensions[col].width=20

    buf=io.BytesIO(); wb.save(buf); buf.seek(0)
    return buf

# ── OSDE ──────────────────────────────────────────────────────────────────────

def build_osde_excel(data, mes, anio):
    car=data['caratula']; pre=data['pre']; pago=data['pago']; nr=data['nr']
    fecha_pres=parse_date(car['fecha_cierre'])
    dias_pago=days_diff(fecha_pres,parse_date(pago['fecha_pago']))
    dias_nr=days_diff(fecha_pres,parse_date(nr['nr_fecha']))
    total_ret=pre['retencion_fdo_res']+pre['ret_col_art12']
    total_pagado=pre['neto_cobrar']+nr['nr_monto']
    dias_prom=(pre['neto_cobrar']*dias_pago+nr['nr_monto']*dias_nr)/total_pagado if total_pagado else 0
    dif_nr=nr['nr_monto']-pre['notas_credito']
    periodo=f'{mes}/{anio}'

    wb=Workbook(); ws=wb.active; ws.title='Reporte'
    setup_ws(ws); header_bg(ws)

    ws.merge_cells('B2:C3'); c(ws,'B2','OSDE',bold=True,size=28,color=WHITE,fill=DARK_BLUE,halign='center')
    ws.merge_cells('E2:F3'); c(ws,'E2',periodo,bold=True,size=20,color=WHITE,fill=DARK_BLUE,halign='center')
    c(ws,'H2','RECETAS',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'H3',car['nro_recetas'],bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center',num_fmt='#,##0')
    c(ws,'I2','FECHA DE PRESENTACION',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'I3',fecha_pres.strftime('%d/%m/%Y'),bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center')
    c(ws,'K2','DÍAS PROM.',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'K3',round(dias_prom,1),bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center',num_fmt='#,##0.0')

    ws.merge_cells('B5:C5'); c(ws,'B5','IMPORTE TOTAL',size=9,color=WHITE,fill=MID_BLUE,halign='center')
    ws.merge_cells('B6:C6'); ni(ws,'B6',car['importe_total']); ws['B6'].font=Font(bold=True,size=13,color=WHITE); ws['B6'].fill=PatternFill('solid',fgColor=MID_BLUE); ws['B6'].alignment=Alignment(horizontal='center',vertical='center')
    ws.merge_cells('E5:F5'); c(ws,'E5','A/C OSDE',size=9,color=WHITE,fill=MID_BLUE,halign='center')
    ws.merge_cells('E6:F6'); ni(ws,'E6',car['a_cargo_osde']); ws['E6'].font=Font(bold=True,size=13,color=WHITE); ws['E6'].fill=PatternFill('solid',fgColor=MID_BLUE); ws['E6'].alignment=Alignment(horizontal='center',vertical='center')
    ws.merge_cells('H5:I5'); c(ws,'H5','AFILIADO',size=9,color=WHITE,fill=MID_BLUE,halign='center')
    ws.merge_cells('H6:I6'); n(ws,'H6',car['afiliado']); ws['H6'].font=Font(bold=True,size=13,color=WHITE); ws['H6'].fill=PatternFill('solid',fgColor=MID_BLUE); ws['H6'].alignment=Alignment(horizontal='center',vertical='center')
    ws.merge_cells('K5:L5'); c(ws,'K5','TOTAL PAGADO OSDE',size=9,color=WHITE,fill=GREEN,halign='center')
    ws.merge_cells('K6:L6'); ni(ws,'K6',total_pagado); ws['K6'].font=Font(bold=True,size=13,color=WHITE); ws['K6'].fill=PatternFill('solid',fgColor=GREEN); ws['K6'].alignment=Alignment(horizontal='center',vertical='center')

    for col,col_dias,lbl,dias,val in [('B','C','LIQ. FINAL',dias_pago,pre['neto_cobrar']),('E','F','NOTAS RECUP.',dias_nr,nr['nr_monto'])]:
        c(ws,f'{col}8',lbl,size=9,color=WHITE,fill=MID_BLUE,halign='center')
        c(ws,f'{col_dias}8','DIAS DE PAGO',size=9,color=WHITE,fill=MID_BLUE,halign='center')
        n(ws,f'{col}9',val); ws[f'{col}9'].font=Font(bold=True,size=13,color=WHITE); ws[f'{col}9'].fill=PatternFill('solid',fgColor=MID_BLUE); ws[f'{col}9'].alignment=Alignment(horizontal='center',vertical='center')
        c(ws,f'{col_dias}9',dias,bold=True,size=13,color=WHITE,fill=MID_BLUE,halign='center')

    ws.merge_cells('B11:C11'); c(ws,'B11','PAGOS A FARMACIA',bold=True,size=11,halign='center')
    ws.merge_cells('E11:F11'); c(ws,'E11','DESCUENTOS',bold=True,size=11,halign='center')
    ws.merge_cells('H11:L11'); c(ws,'H11','PAGOS A DROGUERIA',bold=True,size=11,halign='center')

    ws.merge_cells('B12:C12'); c(ws,'B12','LIQUIDACIÓN',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'B13','Total a verificar:'); n(ws,'C13',car['total_verificar'])
    c(ws,'B14','NETO A COBRAR',bold=True); n(ws,'C14',pre['neto_cobrar'])
    c(ws,'B15','Fecha pago'); d(ws,'C15',parse_date(pago['fecha_pago']))
    c(ws,'B16','Comprobante'); ws['C16'].value=pago['nro_comprobante_pago']; ws['C16'].alignment=Alignment(horizontal='right',vertical='center'); ws['C16'].font=Font(size=10)
    box(ws,11,2,16,3)

    ws.merge_cells('E12:F12'); c(ws,'E12','BONIFICACIONES',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E13','Total:'); n(ws,'F13',car['bonificacion'])
    ws.merge_cells('E14:F14'); c(ws,'E14','RETENCIONES',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E15','Fdo. Res.:'); n(ws,'F15',pre['retencion_fdo_res'])
    c(ws,'E16','Colegio Art. 12 SU:'); n(ws,'F16',pre['ret_col_art12'])
    c(ws,'E17','TOTAL',bold=True); n(ws,'F17',total_ret)
    ws.merge_cells('E18:F18'); c(ws,'E18','AJUSTE FACTURACIÓN',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E19','Débito:'); n(ws,'F19',pre['ajuste_facturacion'])
    box(ws,11,5,19,6)

    ws.merge_cells('H12:L12'); c(ws,'H12','NOTAS DE CRÉDITO',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'I13','Monto Calculado',bold=True,halign='center'); c(ws,'K13','Monto según NR',bold=True,halign='center'); c(ws,'L13','Diferencia',bold=True,halign='center')
    c(ws,'H14','TOTAL',bold=True); n(ws,'I14',pre['notas_credito']); n(ws,'K14',nr['nr_monto']); n(ws,'L14',dif_nr)
    c(ws,'H15','Fecha NR:'); d(ws,'I15',parse_date(nr['nr_fecha']))
    c(ws,'H16','Días:'); ws['I16'].value=dias_nr; ws['I16'].alignment=Alignment(horizontal='right',vertical='center'); ws['I16'].font=Font(size=10)
    box(ws,11,8,16,12)

    ws2=wb.create_sheet('Resumen'); ws2.sheet_view.showGridLines=False
    style=TableStyleInfo(name='TableStyleMedium2',showFirstColumn=False,showLastColumn=False,showRowStripes=True,showColumnStripes=False)
    h1=['RECETAS','IMPORTE TOTAL','A/C OSDE','AFILIADO','%AFL','TOTAL PAGADO','%PAGADO','DIAS PROM. PAGO','AJUSTE FACTURACION','RETENCIONES','%RET','BONIFICACIONES','%BON']
    for i,h in enumerate(h1): ws2.cell(1,i+1,h)
    row2=[car['nro_recetas'],car['importe_total'],car['a_cargo_osde'],car['afiliado'],car['afiliado']/car['importe_total'] if car['importe_total'] else 0,total_pagado,total_pagado/car['a_cargo_osde'] if car['a_cargo_osde'] else 0,round(dias_prom,2),pre['ajuste_facturacion'],total_ret,total_ret/car['a_cargo_osde'] if car['a_cargo_osde'] else 0,car['bonificacion'],car['bonificacion']/car['a_cargo_osde'] if car['a_cargo_osde'] else 0]
    for i,v in enumerate(row2):
        cell=ws2.cell(2,i+1); cell.value=v; cell.number_format='0.00%' if isinstance(v,float) and abs(v)<10 else '#,##0.00'
    tbl1=Table(displayName='tbl_reporte',ref='A1:M2'); tbl1.tableStyleInfo=style; ws2.add_table(tbl1)
    h2=['LIQ FINAL','%OSDE LIQ','DIAS LIQ','NOTAS RECUPERO','%OSDE NR','DIAS NR']
    for i,h in enumerate(h2): ws2.cell(4,i+1,h)
    row5=[pre['neto_cobrar'],pre['neto_cobrar']/total_pagado if total_pagado else 0,dias_pago,nr['nr_monto'],nr['nr_monto']/total_pagado if total_pagado else 0,dias_nr]
    for i,v in enumerate(row5):
        cell=ws2.cell(5,i+1); cell.value=v; cell.number_format='0.00%' if isinstance(v,float) and abs(v)<10 else '#,##0.00'
    tbl2=Table(displayName='tbl_desglose',ref='A4:F5'); tbl2.tableStyleInfo=style; ws2.add_table(tbl2)
    ws2.cell(7,1,'Diferencias NR'); cell=ws2.cell(8,1); cell.value=dif_nr; cell.number_format='#,##0.00'
    tbl3=Table(displayName='tbl_diferencias',ref='A7:A8'); tbl3.tableStyleInfo=style; ws2.add_table(tbl3)
    for col in 'ABCDEFGHIJKLM': ws2.column_dimensions[col].width=20

    buf=io.BytesIO(); wb.save(buf); buf.seek(0)
    return buf

# ── ENDPOINTS ─────────────────────────────────────────────────────────────────

@app.get("/")
def root():
    return {"status": "ok", "service": "Farmacia Merlo - Generador de Reportes"}

@app.post("/reporte/pami")
async def reporte_pami(
    anio: str = Form(...), mes: str = Form(...), quincena: str = Form(...),
    caratula: UploadFile = File(...), opf: UploadFile = File(...),
    pre: UploadFile = File(...), pago: UploadFile = File(...),
    nr: UploadFile = File(...)
):
    car_bytes = await caratula.read()
    opf_bytes = await opf.read()
    pre_bytes = await pre.read()
    pago_bytes = await pago.read()
    nr_bytes = await nr.read()

    periodo = f'{quincena} mes {mes} año {anio}'

    car_data = parse_json(ask_claude(
        pdf_to_content(car_bytes, 'CARÁTULA PAMI') + [{"type":"text","text":f"Período: {periodo}. Extraé: {{\"fecha_cierre\":\"DD/MM/YYYY\",\"nro_recetas\":0,\"total_pvp\":0.0,\"total_pvp_pami\":0.0,\"importe_bruto_convenio\":0.0}}"}],
        SYSTEM_JSON))

    opf_data = parse_json(ask_claude(
        pdf_to_content(opf_bytes, 'OPF PAMI') + [{"type":"text","text":f"Período: {periodo}. Buscar línea Efvo.PAMI. Extraé: {{\"efvo_pami\":0.0,\"fecha_opf\":\"DD/MM/YYYY\",\"nro_comprobante_opf\":0}}"}],
        SYSTEM_JSON))

    pre_data = parse_json(ask_claude(
        pdf_to_content(pre_bytes, 'PRE PAMI') + [{"type":"text","text":"Extraé: {\"deb_cred_os\":0.0,\"bonif_tiras\":0.0,\"bonif_ambulatorio\":0.0,\"bonif_insulinas\":0.0,\"ret_gtos_adm_cofa\":0.0,\"efectivo_drogueria\":0.0,\"fdo_prest_colfarma\":0.0,\"nota_cred_ambulatorio\":0.0,\"nota_cred_insulina\":0.0,\"nota_cred_tiras\":0.0,\"retencion_colegio_art12\":0.0,\"total_liquidacion\":0.0}. Todos los valores deben ser positivos (sin signo negativo). efectivo_drogueria = valor absoluto de EFECTIVO DROGUERIA SALDO."}],
        SYSTEM_JSON))

    pago_data = parse_json(ask_claude(
        pdf_to_content(pago_bytes, 'PAGO FINAL PAMI') + [{"type":"text","text":"Buscar línea PAMI. Extraé: {\"fecha_pago\":\"DD/MM/YYYY\",\"nro_comprobante_pago\":0}"}],
        SYSTEM_JSON))

    nr_text = xls_to_text(nr_bytes, nr.filename)
    nr_data = parse_json(ask_claude(
        [{"type":"text","text":f"NOTAS DE RECUPERO PAMI:\n{nr_text}\n\nLa tabla tiene columnas: TipoCte e ImporteCpte (entre otras). Sumá TODOS los valores de ImporteCpte agrupando por TipoCte (CCF, CCFD, NAF, NRFD, EfSa). Usá la columna ImporteCpte, NO ImporteCo. Fecha de Impresion de primera fila CCF o NAF = fecha_nr. Fecha de primera fila EfSa = fecha_efsa. Extraé: {{\"nr_ccf\":0.0,\"nr_ccfd\":0.0,\"nr_naf\":0.0,\"nr_nrfd\":0.0,\"nr_efsa\":0.0,\"fecha_nr\":\"DD/MM/YYYY\",\"fecha_efsa\":\"DD/MM/YYYY\"}}"}],
        SYSTEM_JSON))

    buf = build_pami_excel(
        {'caratula': car_data, 'opf': opf_data, 'pre': pre_data, 'pago': pago_data, 'nr': nr_data},
        quincena, mes, anio[-2:]
    )
    filename = f"{anio[-2:]}.{mes}.{quincena} - Reporte.xlsx"
    return StreamingResponse(buf, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                             headers={'Content-Disposition': f'attachment; filename="{filename}"'})

@app.post("/reporte/ioma")
async def reporte_ioma(
    anio: str = Form(...), mes: str = Form(...),
    agudo: UploadFile = File(...), opf: UploadFile = File(...),
    pre: UploadFile = File(...), pago: UploadFile = File(...),
    nr: UploadFile = File(...),
    planes: list[UploadFile] = File(default=[])
):
    agudo_bytes = await agudo.read()
    opf_bytes = await opf.read()
    pre_bytes = await pre.read()
    pago_bytes = await pago.read()
    nr_bytes = await nr.read()

    agudo_data = parse_json(ask_claude(
        pdf_to_content(agudo_bytes, 'AGUDO/CRÓNICO IOMA') + [{"type":"text","text":"Buscar RX ON LINE/TOTALIDAD DE LAS RECETAS. Si es un Resumen de Facturación del Colegio, la fecha_cierre es la \"Fecha de Proceso\". Si es una Carátula On-Line, la fecha_cierre es la \"Fecha de generación\". Extraé: {\"fecha_cierre\":\"DD/MM/YYYY\",\"recetas\":0,\"importe100\":0.0,\"ac_instituto\":0.0}"}],
        SYSTEM_JSON))

    planes_data = {
        'AGUDO/CRÓNICO': {'recetas': agudo_data['recetas'], 'importe100': agudo_data['importe100'], 'ac_instituto': agudo_data['ac_instituto']},
        'RECURSOS DE AMPARO': {'recetas':0,'importe100':0,'ac_instituto':0},
        'RESOLUCIÓN DE DIRECTORIO': {'recetas':0,'importe100':0,'ac_instituto':0},
        'MAMI': {'recetas':0,'importe100':0,'ac_instituto':0},
        'MAYOR COBERTURA': {'recetas':0,'importe100':0,'ac_instituto':0},
        'VACUNAS': {'recetas':0,'importe100':0,'ac_instituto':0},
    }

    for plan_file in planes:
        pb = await plan_file.read()
        # Detectar si es Resumen del Colegio (múltiples planes) o carátula individual
        tipo_data = parse_json(ask_claude(
            pdf_to_content(pb, 'DOCUMENTO PLAN IOMA') + [{"type":"text","text":"Este documento es un Resumen de Facturación del Colegio de Farmacéuticos con tabla de múltiples planes, O es una carátula individual de un solo plan IOMA. Respondé SOLO: {\"tipo\":\"resumen_colegio\"} o {\"tipo\":\"caratula_individual\"}"}],
            SYSTEM_JSON))
        if tipo_data.get('tipo') == 'resumen_colegio':
            # Resumen del Colegio: extraer todos los planes de la tabla
            planes_lista = parse_json(ask_claude(
                pdf_to_content(pb, 'RESUMEN COLEGIO IOMA') + [{"type":"text","text":"Extraé cada fila de la tabla de planes. Para cada plan: importe100=Imp.Total, ac_instituto=Imp.Os. Respondé: {\"planes\":[{\"plan\":\"nombre del convenio/plan\",\"recetas\":0,\"importe100\":0.0,\"ac_instituto\":0.0}]}"}],
                SYSTEM_JSON))
            for p in planes_lista.get('planes', []):
                plan_name = p.get('plan','').upper()
                entry = {'recetas':p['recetas'],'importe100':p['importe100'],'ac_instituto':p['ac_instituto']}
                if 'MAMI' in plan_name: planes_data['MAMI'] = entry
                elif 'MAYOR' in plan_name: planes_data['MAYOR COBERTURA'] = entry
                elif 'AMPARO' in plan_name: planes_data['RECURSOS DE AMPARO'] = entry
                elif 'DIRECTORIO' in plan_name: planes_data['RESOLUCIÓN DE DIRECTORIO'] = entry
                elif 'VACUNA' in plan_name: planes_data['VACUNAS'] = entry
        else:
            # Carátula individual de un solo plan
            pd_data = parse_json(ask_claude(
                pdf_to_content(pb, 'CARÁTULA PLAN IOMA') + [{"type":"text","text":"Leer Convenio/Plan para identificar el plan. Extraé: {\"plan\":\"nombre del plan\",\"recetas\":0,\"importe100\":0.0,\"ac_instituto\":0.0}"}],
                SYSTEM_JSON))
            plan_name = pd_data.get('plan','').upper()
            entry = {'recetas':pd_data['recetas'],'importe100':pd_data['importe100'],'ac_instituto':pd_data['ac_instituto']}
            if 'MAMI' in plan_name: planes_data['MAMI'] = entry
            elif 'MAYOR' in plan_name: planes_data['MAYOR COBERTURA'] = entry
            elif 'AMPARO' in plan_name: planes_data['RECURSOS DE AMPARO'] = entry
            elif 'DIRECTORIO' in plan_name: planes_data['RESOLUCIÓN DE DIRECTORIO'] = entry
            elif 'VACUNA' in plan_name: planes_data['VACUNAS'] = entry

    opf_data = parse_json(ask_claude(
        pdf_to_content(opf_bytes, 'OPF IOMA') + [{"type":"text","text":"Efvo.IOMA AMBULATORIO = anticipo. Sección RETENCIONES línea RGI = ing_brutos_anticipo. Extraé: {\"efvo_ioma\":0.0,\"fecha_opf\":\"DD/MM/YYYY\",\"nro_comprobante_opf\":0,\"ing_brutos_anticipo\":0.0}"}],
        SYSTEM_JSON))

    pre_data = parse_json(ask_claude(
        pdf_to_content(pre_bytes, 'PRE IOMA') + [{"type":"text","text":"Extraé: {\"deb_cred_os\":0.0,\"bonificaciones\":0.0,\"fdo_prest_colfarma\":0.0,\"nrf_ant\":0.0,\"nrf_def\":0.0,\"nrf_directas\":0.0,\"retencion_colegio_art12\":0.0,\"total_liquidacion\":0.0}"}],
        SYSTEM_JSON))

    pago_data = parse_json(ask_claude(
        pdf_to_content(pago_bytes, 'PAGO FINAL IOMA') + [{"type":"text","text":"Sección RETENCIONES línea RGI = ing_brutos_pago. Extraé: {\"fecha_pago\":\"DD/MM/YYYY\",\"nro_comprobante_pago\":0,\"ing_brutos_pago\":0.0}"}],
        SYSTEM_JSON))

    nr_text = xls_to_text(nr_bytes, nr.filename)
    nr_data = parse_json(ask_claude(
        [{"type":"text","text":f"NOTAS DE RECUPERO IOMA:\n{nr_text}\n\nSumá NRF y NRFD agrupados por fecha de Impresion. Extraé: {{\"nr_por_fecha\":[{{\"fecha\":\"DD/MM/YYYY\",\"monto\":0.0}}]}}"}],
        SYSTEM_JSON))

    buf = build_ioma_excel(
        {'planes': planes_data, 'opf': opf_data, 'pre': pre_data, 'pago': pago_data, 'nr': nr_data, 'fecha_cierre': agudo_data['fecha_cierre']},
        mes, anio[-2:]
    )
    filename = f"{anio[-2:]}.{mes} - Reporte IOMA.xlsx"
    return StreamingResponse(buf, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                             headers={'Content-Disposition': f'attachment; filename="{filename}"'})

@app.post("/reporte/osde")
async def reporte_osde(
    anio: str = Form(...), mes: str = Form(...),
    caratula: UploadFile = File(...), pre: UploadFile = File(...),
    pago: UploadFile = File(...), nr: UploadFile = File(...)
):
    car_bytes = await caratula.read()
    pre_bytes = await pre.read()
    pago_bytes = await pago.read()
    nr_bytes = await nr.read()

    car_data = parse_json(ask_claude(
        pdf_to_content(car_bytes, 'CARÁTULA OSDE') + [{"type":"text","text":"Extraé: {\"fecha_cierre\":\"DD/MM/YYYY\",\"nro_recetas\":0,\"importe_total\":0.0,\"afiliado\":0.0,\"a_cargo_osde\":0.0,\"bonificacion\":0.0,\"total_verificar\":0.0}"}],
        SYSTEM_JSON))

    pre_data = parse_json(ask_claude(
        pdf_to_content(pre_bytes, 'PRE OSDE') + [{"type":"text","text":"La tabla tiene columnas: Concepto, Base Cálculo, Créditos, Débitos. REGLAS: 1) Para retencion_fdo_res, ret_col_art12 y notas_credito tomá SIEMPRE el valor de la columna Débitos, NO la Base de Cálculo. 2) Para ajuste_facturacion: puede haber varias líneas de Ajuste Facturación. Identificá pares que se cancelan entre sí (mismo monto, uno en Débitos y otro en Créditos) y excluílos. El ajuste_facturacion es el monto de la línea que NO tiene contraparte que la cancele (si es débito es positivo, si es crédito es negativo). Si no hay ajuste real, ajuste_facturacion=0. 3) neto_cobrar = fila Neto a Cobrar columna Créditos. Extraé: {\"nro_liquidacion\":0,\"ajuste_facturacion\":0.0,\"retencion_fdo_res\":0.0,\"ret_col_art12\":0.0,\"notas_credito\":0.0,\"neto_cobrar\":0.0}"}],
        SYSTEM_JSON))

    pago_data = parse_json(ask_claude(
        pdf_to_content(pago_bytes, 'PAGO FINAL OSDE') + [{"type":"text","text":f"La fecha_pago es la fecha del ENCABEZADO del documento (campo Fecha:), NO la fecha de presentación de la tabla. El nro_comprobante_pago es el número de Orden de Pago del encabezado. Confirmar que existe línea OSDE con liquidación nro {pre_data.get('nro_liquidacion','')}. Extraé: {{\"fecha_pago\":\"DD/MM/YYYY\",\"nro_comprobante_pago\":0}}"}],
        SYSTEM_JSON))

    nr_data = parse_json(ask_claude(
        pdf_to_content(nr_bytes, 'NOTA DE CRÉDITO DEL SUD') + [{"type":"text","text":"Tomar el TOTAL del documento. Extraé: {\"nr_monto\":0.0,\"nr_fecha\":\"DD/MM/YYYY\"}"}],
        SYSTEM_JSON))

    buf = build_osde_excel(
        {'caratula': car_data, 'pre': pre_data, 'pago': pago_data, 'nr': nr_data},
        mes, anio[-2:]
    )
    filename = f"{anio[-2:]}.{mes} - Reporte OSDE.xlsx"
    return StreamingResponse(buf, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                             headers={'Content-Disposition': f'attachment; filename="{filename}"'})

# ── OSPECON ───────────────────────────────────────────────────────────────────

def build_ospecon_excel(data, mes, anio):
    car=data['caratula']; pre=data['pre']; pago=data['pago']
    fecha_pres=parse_date(car['fecha_cierre'])
    dias_pago=days_diff(fecha_pres,parse_date(pago['fecha_pago']))
    total_ret=abs(pre['retencion_fdo_res'])+abs(pre['ret_col_art12'])
    afiliado=car['importe_total']-car['ac_os']
    ajuste=pre.get('ajuste_facturacion',0)
    periodo=f'{mes}/{anio}'

    wb=Workbook(); ws=wb.active; ws.title='Reporte'
    setup_ws(ws); header_bg(ws)

    ws.merge_cells('B2:C3'); c(ws,'B2','OSPECON',bold=True,size=22,color=WHITE,fill=DARK_BLUE,halign='center')
    ws.merge_cells('E2:F3'); c(ws,'E2',periodo,bold=True,size=20,color=WHITE,fill=DARK_BLUE,halign='center')
    c(ws,'H2','RECETAS',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'H3',car['nro_recetas'],bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center',num_fmt='#,##0')
    c(ws,'I2','FECHA DE PRESENTACION',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'I3',fecha_pres.strftime('%d/%m/%Y'),bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center')
    c(ws,'K2','DÍAS PROM.',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'K3',round(dias_pago),bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center',num_fmt='#,##0')

    ws.merge_cells('B5:C5'); c(ws,'B5','IMPORTE TOTAL',size=9,color=WHITE,fill=MID_BLUE,halign='center')
    ws.merge_cells('B6:C6'); ni(ws,'B6',car['importe_total']); ws['B6'].font=Font(bold=True,size=13,color=WHITE); ws['B6'].fill=PatternFill('solid',fgColor=MID_BLUE); ws['B6'].alignment=Alignment(horizontal='center',vertical='center')
    ws.merge_cells('E5:F5'); c(ws,'E5','A/C OSPECON',size=9,color=WHITE,fill=MID_BLUE,halign='center')
    ws.merge_cells('E6:F6'); ni(ws,'E6',car['ac_os']); ws['E6'].font=Font(bold=True,size=13,color=WHITE); ws['E6'].fill=PatternFill('solid',fgColor=MID_BLUE); ws['E6'].alignment=Alignment(horizontal='center',vertical='center')
    ws.merge_cells('H5:I5'); c(ws,'H5','AFILIADO',size=9,color=WHITE,fill=MID_BLUE,halign='center')
    ws.merge_cells('H6:I6'); ni(ws,'H6',afiliado); ws['H6'].font=Font(bold=True,size=13,color=WHITE); ws['H6'].fill=PatternFill('solid',fgColor=MID_BLUE); ws['H6'].alignment=Alignment(horizontal='center',vertical='center')
    ws.merge_cells('K5:L5'); c(ws,'K5','TOTAL PAGADO OSPECON',size=9,color=WHITE,fill=GREEN,halign='center')
    ws.merge_cells('K6:L6'); ni(ws,'K6',pre['neto_cobrar']); ws['K6'].font=Font(bold=True,size=13,color=WHITE); ws['K6'].fill=PatternFill('solid',fgColor=GREEN); ws['K6'].alignment=Alignment(horizontal='center',vertical='center')

    c(ws,'B8','LIQ. FINAL',size=9,color=WHITE,fill=MID_BLUE,halign='center')
    c(ws,'C8','DIAS DE PAGO',size=9,color=WHITE,fill=MID_BLUE,halign='center')
    ni(ws,'B9',pre['neto_cobrar']); ws['B9'].font=Font(bold=True,size=13,color=WHITE); ws['B9'].fill=PatternFill('solid',fgColor=MID_BLUE); ws['B9'].alignment=Alignment(horizontal='center',vertical='center')
    c(ws,'C9',dias_pago,bold=True,size=13,color=WHITE,fill=MID_BLUE,halign='center')

    ws.merge_cells('B11:C11'); c(ws,'B11','PAGOS A FARMACIA',bold=True,size=11,halign='center')
    ws.merge_cells('E11:F11'); c(ws,'E11','DESCUENTOS',bold=True,size=11,halign='center')

    ws.merge_cells('B12:C12'); c(ws,'B12','LIQUIDACIÓN',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'B13','NETO A COBRAR',bold=True); n(ws,'C13',pre['neto_cobrar'])
    c(ws,'B14','Fecha pago'); d(ws,'C14',parse_date(pago['fecha_pago']))
    c(ws,'B15','Comprobante'); ws['C15'].value=pago['nro_comprobante_pago']; ws['C15'].alignment=Alignment(horizontal='right',vertical='center'); ws['C15'].font=Font(size=10)
    box(ws,11,2,15,3)

    ws.merge_cells('E12:F12'); c(ws,'E12','BONIFICACIONES',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E13','Total:'); n(ws,'F13',abs(pre['bonificacion']))
    ws.merge_cells('E14:F14'); c(ws,'E14','RETENCIONES',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E15','Fdo. Res.:'); n(ws,'F15',abs(pre['retencion_fdo_res']))
    c(ws,'E16','Colegio Art. 12 SU:'); n(ws,'F16',abs(pre['ret_col_art12']))
    c(ws,'E17','TOTAL',bold=True); n(ws,'F17',total_ret)
    ws.merge_cells('E18:F18'); c(ws,'E18','AJUSTE FACTURACIÓN',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E19','Débito:'); n(ws,'F19',ajuste)
    box(ws,11,5,19,6)

    ws2=wb.create_sheet('Resumen'); ws2.sheet_view.showGridLines=False
    style=TableStyleInfo(name='TableStyleMedium2',showFirstColumn=False,showLastColumn=False,showRowStripes=True,showColumnStripes=False)
    h1=['RECETAS','IMPORTE TOTAL','A/C OSPECON','AFILIADO','%AFL','TOTAL PAGADO','%PAGADO','DIAS PAGO','RETENCIONES','%RET','BONIFICACIONES','%BON']
    for i,h in enumerate(h1): ws2.cell(1,i+1,h)
    row2=[car['nro_recetas'],car['importe_total'],car['ac_os'],afiliado,
          afiliado/car['importe_total'] if car['importe_total'] else 0,
          pre['neto_cobrar'],pre['neto_cobrar']/car['ac_os'] if car['ac_os'] else 0,
          dias_pago,total_ret,total_ret/car['ac_os'] if car['ac_os'] else 0,
          abs(pre['bonificacion']),abs(pre['bonificacion'])/car['ac_os'] if car['ac_os'] else 0]
    for i,v in enumerate(row2):
        cell=ws2.cell(2,i+1); cell.value=v
        cell.number_format='0.00%' if isinstance(v,float) and abs(v)<10 else '#,##0.00'
    tbl1=Table(displayName='tbl_ospecon',ref='A1:L2'); tbl1.tableStyleInfo=style; ws2.add_table(tbl1)
    for col in 'ABCDEFGHIJKL': ws2.column_dimensions[col].width=20

    buf=io.BytesIO(); wb.save(buf); buf.seek(0)
    return buf

@app.post("/reporte/ospecon")
async def reporte_ospecon(
    anio: str = Form(...), mes: str = Form(...),
    caratula: UploadFile = File(...), pre: UploadFile = File(...),
    pago: UploadFile = File(...)
):
    car_bytes = await caratula.read()
    pre_bytes = await pre.read()
    pago_bytes = await pago.read()

    car_data = parse_json(ask_claude(
        pdf_to_content(car_bytes, 'CARÁTULA OSPECON') + [{"type":"text","text":"La fecha_cierre es la \"Fecha de generación\", NO la \"Fecha de Proceso\". Extraé: {\"fecha_cierre\":\"DD/MM/YYYY\",\"nro_recetas\":0,\"importe_total\":0.0,\"ac_os\":0.0}"}],
        SYSTEM_JSON))

    pre_data = parse_json(ask_claude(
        pdf_to_content(pre_bytes, 'PRE OSPECON') + [{"type":"text","text":"Columnas: Concepto, Base Cálculo, Créditos, Débitos. Para bonificacion, retencion_fdo_res y ret_col_art12 tomá SIEMPRE la columna Débitos. Para ajuste_facturacion: puede haber varias líneas de Ajuste Facturación, identificá pares que se cancelan entre sí (mismo monto, uno Débitos y otro Créditos) y excluílos, el ajuste_facturacion es el monto de la línea que NO tiene contraparte (positivo si débito, negativo si crédito), 0 si no hay. neto_cobrar = fila Neto a Cobrar columna Créditos. Extraé: {\"nro_liquidacion\":0,\"ajuste_facturacion\":0.0,\"bonificacion\":0.0,\"retencion_fdo_res\":0.0,\"ret_col_art12\":0.0,\"neto_cobrar\":0.0}"}],
        SYSTEM_JSON))

    pago_data = parse_json(ask_claude(
        pdf_to_content(pago_bytes, 'PAGO FINAL OSPECON') + [{"type":"text","text":f"La fecha_pago es la fecha del ENCABEZADO del documento (campo Fecha:). El nro_comprobante_pago es el número de Orden de Pago del encabezado. Confirmar línea OSPECON con liquidación nro {pre_data.get('nro_liquidacion','')}. Extraé: {{\"fecha_pago\":\"DD/MM/YYYY\",\"nro_comprobante_pago\":0}}"}],
        SYSTEM_JSON))

    buf = build_ospecon_excel(
        {'caratula': car_data, 'pre': pre_data, 'pago': pago_data},
        mes, anio[-2:]
    )
    filename = f"{anio[-2:]}.{mes} - Reporte OSPECON.xlsx"
    return StreamingResponse(buf, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                             headers={'Content-Disposition': f'attachment; filename="{filename}"'})

# ── OSPRERA ───────────────────────────────────────────────────────────────────

def build_osprera_excel(data, mes, anio):
    planes = data['planes']; pre = data['pre']; pago = data['pago']
    opf = data.get('opf'); nr_data = data.get('nr')
    con_quincena = data.get('con_quincena', False)
    quincena = data.get('quincena', '')
    fecha_pres = parse_date(data.get('fecha_cierre') or pre['fecha_presentacion'])
    dias_pago = days_diff(fecha_pres, parse_date(pago['fecha_pago']))

    # Con quincena: puede haber OPF y NR
    dias_ant = 0; total_nr = 0; dias_nr_pond = 0
    if con_quincena and opf:
        dias_ant = days_diff(fecha_pres, parse_date(opf['fecha_opf']))
    if con_quincena and nr_data:
        nr_por_fecha = nr_data.get('nr_por_fecha', [])
        total_pond = 0
        for item in nr_por_fecha:
            m = item.get('monto', 0); total_nr += m
            total_pond += m * days_diff(fecha_pres, parse_date(item.get('fecha', '')))
        dias_nr_pond = total_pond / total_nr if total_nr else 0

    total_recetas = sum(p.get('recetas', 0) for p in planes.values())
    total_importe100 = sum(p.get('importe100', 0) for p in planes.values())
    total_ac = sum(p.get('ac_os', 0) for p in planes.values())
    afiliado = total_importe100 - total_ac

    deb_os = abs(pre['deb_cred_os']) if pre['deb_cred_os'] < 0 else 0
    cred_os = pre['deb_cred_os'] if pre['deb_cred_os'] > 0 else 0
    bonificaciones = abs(pre['bonificaciones'])
    ret_cofa = abs(pre['fdo_prest_colfarma']) + abs(pre['retencion_colegio_art12'])
    efvo_opf = opf['efvo_osprera'] if (con_quincena and opf) else 0
    liq_final = pre['total_liquidacion'] - efvo_opf
    total_pagado = efvo_opf + liq_final + total_nr
    if total_pagado == 0: total_pagado = pre['total_liquidacion']
    # dias prom ponderado si hay OPF o NR
    if con_quincena and (efvo_opf or total_nr):
        componentes = [(efvo_opf, dias_ant), (liq_final, dias_pago), (total_nr, dias_nr_pond)]
        total_pond2 = sum(v*d for v,d in componentes)
        dias_prom = total_pond2 / total_pagado if total_pagado else dias_pago
    else:
        dias_prom = dias_pago
    periodo = f'{mes}/{anio}' + (f' {quincena}' if con_quincena and quincena else '')
    planes_activos = {k: v for k, v in planes.items() if v.get('recetas', 0) > 0}

    wb = Workbook(); ws = wb.active; ws.title = 'Reporte'
    setup_ws(ws); header_bg(ws)

    ws.merge_cells('B2:C3'); c(ws,'B2','OSPRERA',bold=True,size=22,color=WHITE,fill=DARK_BLUE,halign='center')
    ws.merge_cells('E2:F3'); c(ws,'E2',periodo,bold=True,size=20,color=WHITE,fill=DARK_BLUE,halign='center')
    c(ws,'H2','RECETAS',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'H3',total_recetas,bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center',num_fmt='#,##0')
    c(ws,'I2','FECHA DE PRESENTACION',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'I3',fecha_pres.strftime('%d/%m/%Y'),bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center')
    c(ws,'K2','DÍAS PROM.',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'K3',round(dias_prom,1),bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center',num_fmt='#,##0.0')

    for coord,label,val,clr in [('B5:C5','IMPORTE 100%',total_importe100,MID_BLUE),('E5:F5','A/C OSPRERA',total_ac,MID_BLUE),('H5:I5','AFILIADO',afiliado,MID_BLUE),('K5:L5','TOTAL PAGADO OSPRERA',total_pagado,GREEN)]:
        ws.merge_cells(coord); start=coord.split(':')[0]; end=coord.split(':')[1]
        r=int(start[1]); c_letter=start[0]; end_letter=end[0]
        c(ws,f'{c_letter}{r}',label,size=9,color=WHITE,fill=clr,halign='center')
        coord6=f'{c_letter}{r+1}:{end_letter}{r+1}'; ws.merge_cells(coord6)
        ni(ws,f'{c_letter}{r+1}',val); ws[f'{c_letter}{r+1}'].font=Font(bold=True,size=13,color=WHITE); ws[f'{c_letter}{r+1}'].fill=PatternFill('solid',fgColor=clr); ws[f'{c_letter}{r+1}'].alignment=Alignment(horizontal='center',vertical='center')

    if con_quincena and efvo_opf:
        for col,col_dias,lbl,dias,val in [('B','C','ANTICIPO',dias_ant,efvo_opf),('E','F','LIQ. FINAL',dias_pago,liq_final)]:
            c(ws,f'{col}8',lbl,size=9,color=WHITE,fill=MID_BLUE,halign='center')
            c(ws,f'{col_dias}8','DIAS DE PAGO',size=9,color=WHITE,fill=MID_BLUE,halign='center')
            ni(ws,f'{col}9',val); ws[f'{col}9'].font=Font(bold=True,size=13,color=WHITE); ws[f'{col}9'].fill=PatternFill('solid',fgColor=MID_BLUE); ws[f'{col}9'].alignment=Alignment(horizontal='center',vertical='center')
            c(ws,f'{col_dias}9',dias,bold=True,size=13,color=WHITE,fill=MID_BLUE,halign='center')
        if total_nr:
            c(ws,'H8','NOTAS RECUP.',size=9,color=WHITE,fill=MID_BLUE,halign='center')
            c(ws,'I8','DIAS DE PAGO',size=9,color=WHITE,fill=MID_BLUE,halign='center')
            ni(ws,'H9',total_nr); ws['H9'].font=Font(bold=True,size=13,color=WHITE); ws['H9'].fill=PatternFill('solid',fgColor=MID_BLUE); ws['H9'].alignment=Alignment(horizontal='center',vertical='center')
            c(ws,'I9',round(dias_nr_pond,1),bold=True,size=13,color=WHITE,fill=MID_BLUE,halign='center')
    else:
        c(ws,'B8','LIQ. FINAL',size=9,color=WHITE,fill=MID_BLUE,halign='center')
        c(ws,'C8','DIAS DE PAGO',size=9,color=WHITE,fill=MID_BLUE,halign='center')
        ni(ws,'B9',total_pagado); ws['B9'].font=Font(bold=True,size=13,color=WHITE); ws['B9'].fill=PatternFill('solid',fgColor=MID_BLUE); ws['B9'].alignment=Alignment(horizontal='center',vertical='center')
        c(ws,'C9',dias_pago,bold=True,size=13,color=WHITE,fill=MID_BLUE,halign='center')

    ws.merge_cells('B11:C11'); c(ws,'B11','CARÁTULAS',bold=True,size=11,halign='center')
    ws.merge_cells('E11:F11'); c(ws,'E11','DESCUENTOS',bold=True,size=11,halign='center')
    ws.merge_cells('H11:L11'); c(ws,'H11','PAGOS A FARMACIA',bold=True,size=11,halign='center')

    ws.merge_cells('B12:C12'); c(ws,'B12','COMPOSICIÓN POR PLAN',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'B13','PLAN',bold=True); c(ws,'C13','RECETAS',bold=True,halign='right')
    row = 14
    for plan, datos in planes_activos.items():
        c(ws,f'B{row}',plan); ws[f'C{row}'].value=datos['recetas']; ws[f'C{row}'].alignment=Alignment(horizontal='right',vertical='center'); ws[f'C{row}'].font=Font(size=10); row+=1
    c(ws,f'B{row}','TOTAL',bold=True); ws[f'C{row}'].value=total_recetas; ws[f'C{row}'].alignment=Alignment(horizontal='right',vertical='center'); ws[f'C{row}'].font=Font(bold=True,size=10)
    row_end_car=row; box(ws,11,2,row_end_car,3)

    ws.merge_cells('E12:F12'); c(ws,'E12','BONIFICACIONES',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E13','Total:'); n(ws,'F13',bonificaciones)
    ws.merge_cells('E14:F14'); c(ws,'E14','RETENCIONES',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E15','Fdo Prest. COLFARMA:'); n(ws,'F15',abs(pre['fdo_prest_colfarma']))
    c(ws,'E16','Colegio Art. 12 SU:'); n(ws,'F16',abs(pre['retencion_colegio_art12']))
    c(ws,'E17','TOTAL',bold=True); n(ws,'F17',ret_cofa)
    ws.merge_cells('E18:F18'); c(ws,'E18','DÉB. / CRÉD. OS',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E19','Débito OS:'); n(ws,'F19',deb_os)
    c(ws,'E20','Crédito OS:'); n(ws,'F20',cred_os)
    box(ws,11,5,20,6)

    if con_quincena and opf:
        ws.merge_cells('H12:L12'); c(ws,'H12','ANTICIPO (OPF)',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
        c(ws,'H13','TOTAL',bold=True); n(ws,'I13',efvo_opf)
        c(ws,'H14','Fecha pago'); d(ws,'I14',parse_date(opf['fecha_opf']))
        c(ws,'H15','Comprobante'); ws['I15'].value=opf['nro_comprobante_opf']; ws['I15'].alignment=Alignment(horizontal='right',vertical='center'); ws['I15'].font=Font(size=10)
        ws.merge_cells('H16:L16'); c(ws,'H16','LIQUIDACIÓN',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
        c(ws,'H17','Bruto a pagar antes imp.:'); n(ws,'I17',pre['total_liquidacion'])
        c(ws,'H18','TOTAL',bold=True); n(ws,'I18',liq_final)
        c(ws,'H19','Fecha pago'); d(ws,'I19',parse_date(pago['fecha_pago']))
        c(ws,'H20','Comprobante'); ws['I20'].value=pago['nro_comprobante_pago']; ws['I20'].alignment=Alignment(horizontal='right',vertical='center'); ws['I20'].font=Font(size=10)
        box_end = 20
        if total_nr and nr_data:
            nr_por_fecha = nr_data.get('nr_por_fecha', [])
            ws.merge_cells(f'H{box_end+1}:L{box_end+1}'); c(ws,f'H{box_end+1}','NOTAS DE RECUPERO',bold=True,size=10,fill=LIGHT_BLUE,halign='center'); box_end+=1
            c(ws,f'H{box_end+1}','Fecha',bold=True,halign='center'); c(ws,f'I{box_end+1}','Monto',bold=True,halign='center'); c(ws,f'K{box_end+1}','Días',bold=True,halign='center'); box_end+=1
            for item in sorted(nr_por_fecha, key=lambda x: x.get('fecha','')):
                df=days_diff(fecha_pres,parse_date(item.get('fecha',''))); d(ws,f'H{box_end+1}',parse_date(item.get('fecha',''))); n(ws,f'I{box_end+1}',item.get('monto',0))
                ws[f'K{box_end+1}'].value=df; ws[f'K{box_end+1}'].alignment=Alignment(horizontal='right',vertical='center'); ws[f'K{box_end+1}'].font=Font(size=10); box_end+=1
            c(ws,f'H{box_end+1}','DÍAS PROM.',bold=True); ws[f'K{box_end+1}'].value=round(dias_nr_pond,1); ws[f'K{box_end+1}'].alignment=Alignment(horizontal='right',vertical='center'); ws[f'K{box_end+1}'].font=Font(bold=True,size=10); box_end+=1
        box(ws,11,8,box_end,12)
    else:
        ws.merge_cells('H12:L12'); c(ws,'H12','LIQUIDACIÓN',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
        c(ws,'H13','TOTAL LIQUIDACIÓN',bold=True); n(ws,'I13',pre['total_liquidacion'])
        c(ws,'H14','Fecha pago'); d(ws,'I14',parse_date(pago['fecha_pago']))
        c(ws,'H15','Comprobante'); ws['I15'].value=pago['nro_comprobante_pago']; ws['I15'].alignment=Alignment(horizontal='right',vertical='center'); ws['I15'].font=Font(size=10)
        box(ws,11,8,15,12)

    ws2=wb.create_sheet('Resumen'); ws2.sheet_view.showGridLines=False
    style=TableStyleInfo(name='TableStyleMedium2',showFirstColumn=False,showLastColumn=False,showRowStripes=True,showColumnStripes=False)
    h1=['RECETAS','IMPORTE 100%','A/C OSPRERA','AFILIADO','%AFL','TOTAL PAGADO','%PAGADO','DIAS PAGO','DEBITOS OS','RETENCIONES','%RET','BONIFICACIONES','%BON']
    for i,h in enumerate(h1): ws2.cell(1,i+1,h)
    row2=[total_recetas,total_importe100,total_ac,afiliado,
          afiliado/total_importe100 if total_importe100 else 0,
          total_pagado,total_pagado/total_ac if total_ac else 0,
          dias_pago,deb_os,ret_cofa,ret_cofa/total_ac if total_ac else 0,
          bonificaciones,bonificaciones/total_ac if total_ac else 0]
    for i,v in enumerate(row2):
        cell=ws2.cell(2,i+1); cell.value=v
        cell.number_format='0.00%' if isinstance(v,float) and abs(v)<10 else '#,##0.00'
    tbl1=Table(displayName='tbl_osprera',ref='A1:M2'); tbl1.tableStyleInfo=style; ws2.add_table(tbl1)

    if con_quincena:
        ws2.cell(4,1,'Diferencias NR'); cell=ws2.cell(5,1); cell.value=dif_nr; cell.number_format='#,##0.00'
        tbl2=Table(displayName='tbl_osprera_dif',ref='A4:A5'); tbl2.tableStyleInfo=style; ws2.add_table(tbl2)

    for col in 'ABCDEFGHIJKLM': ws2.column_dimensions[col].width=20

    buf=io.BytesIO(); wb.save(buf); buf.seek(0)
    return buf

@app.post("/reporte/osprera")
async def reporte_osprera(
    anio: str = Form(...), mes: str = Form(...),
    caratulas: list[UploadFile] = File(...),
    pre: UploadFile = File(...),
    pago: UploadFile = File(...),
    quincena: str = Form(default=None),
    nr: UploadFile = File(default=None)
):
    pre_bytes = await pre.read()
    pago_bytes = await pago.read()
    con_quincena = quincena is not None

    planes_data = {
        'GENERAL': {'recetas':0,'importe100':0.0,'ac_os':0.0},
        'TRATAMIENTO PROLONGADO': {'recetas':0,'importe100':0.0,'ac_os':0.0},
        'MONOTRIBUTISTAS': {'recetas':0,'importe100':0.0,'ac_os':0.0},
        'RURAL': {'recetas':0,'importe100':0.0,'ac_os':0.0},
        'DECLARACIÓN DE DISPENSA': {'recetas':0,'importe100':0.0,'ac_os':0.0},
    }

    fecha_cierre_osprera = None
    for car_file in caratulas:
        cb = await car_file.read()
        car_data = parse_json(ask_claude(
            pdf_to_content(cb, 'CARÁTULA OSPRERA') + [{"type":"text","text":"Identificar el plan exacto leyendo el campo Convenio/Plan. La fecha_cierre es la \"Fecha de generación\", NO la \"Fecha de Proceso\". Extraé: {\"plan\":\"nombre completo del convenio/plan\",\"fecha_cierre\":\"DD/MM/YYYY\",\"nro_recetas\":0,\"importe_total\":0.0,\"ac_os\":0.0}"}],
            SYSTEM_JSON))
        if not fecha_cierre_osprera: fecha_cierre_osprera = car_data.get('fecha_cierre')
        plan_name = car_data.get('plan','').upper()
        if 'MONOT' in plan_name: key='MONOTRIBUTISTAS'
        elif 'RURAL' in plan_name: key='RURAL'
        elif 'PROLONGADO' in plan_name: key='TRATAMIENTO PROLONGADO'
        elif 'DISPENSA' in plan_name: key='DECLARACIÓN DE DISPENSA'
        else: key='GENERAL'
        planes_data[key]['recetas'] += car_data.get('nro_recetas',0)
        planes_data[key]['importe100'] += car_data.get('importe_total',0.0)
        planes_data[key]['ac_os'] += car_data.get('ac_os',0.0)

    nr_data = None
    if con_quincena and nr:
        nr_bytes = await nr.read()
        nr_text = xls_to_text(nr_bytes, nr.filename)
        nr_data = parse_json(ask_claude(
            [{"type":"text","text":f"NOTAS DE RECUPERO OSPRERA:\n{nr_text}\n\nSumá NRF y NRFD agrupados por fecha. Extraé: {{\"nr_por_fecha\":[{{\"fecha\":\"DD/MM/YYYY\",\"monto\":0.0}}]}}"}],
            SYSTEM_JSON))

    pre_data = parse_json(ask_claude(
        pdf_to_content(pre_bytes, 'PRE OSPRERA') + [{"type":"text","text":"Extraé: {\"fecha_presentacion\":\"DD/MM/YYYY\",\"nro_comprobante\":0,\"deb_cred_os\":0.0,\"bonificaciones\":0.0,\"fdo_prest_colfarma\":0.0,\"retencion_colegio_art12\":0.0,\"total_liquidacion\":0.0}. deb_cred_os = DEB/CRED DE OBRA SOCIAL (negativo si es débito). bonificaciones = BONIFICACIONES. fdo_prest_colfarma = FDO PREST COLFARMA. total_liquidacion = Total liquidación."}],
        SYSTEM_JSON))

    pago_data = parse_json(ask_claude(
        pdf_to_content(pago_bytes, 'PAGO FINAL OSPRERA') + [{"type":"text","text":f"La fecha_pago es la Fecha del encabezado del documento. El nro_comprobante_pago es el número de Comprobante del encabezado. Confirmar que existe línea OSPRERA con comprobante {pre_data.get('nro_comprobante','')}. Extraé: {{\"fecha_pago\":\"DD/MM/YYYY\",\"nro_comprobante_pago\":\"\"}}"}],
        SYSTEM_JSON))

    buf = build_osprera_excel(
        {'planes': planes_data, 'pre': pre_data, 'pago': pago_data,
         'fecha_cierre': fecha_cierre_osprera, 'opf': None, 'nr': nr_data,
         'con_quincena': con_quincena, 'quincena': quincena},
        mes, anio[-2:]
    )
    q_str = f".{quincena}" if con_quincena else ""
    filename = f"{anio[-2:]}.{mes}{q_str} - Reporte OSPRERA.xlsx"
    return StreamingResponse(buf, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                             headers={'Content-Disposition': f'attachment; filename="{filename}"'})

# ── UNION PERSONAL ─────────────────────────────────────────────────────────────

def build_unionpersonal_excel(data, mes, anio):
    planes = data['planes']; opf = data['opf']; pre = data['pre']; pago = data['pago']
    fecha_pres = parse_date(data.get('fecha_cierre') or pre['fecha_presentacion'])
    dias_ant = days_diff(fecha_pres, parse_date(opf['fecha_opf']))
    dias_liq = days_diff(fecha_pres, parse_date(pago['fecha_pago']))

    total_recetas = sum(p.get('recetas', 0) for p in planes.values())
    total_importe100 = sum(p.get('importe100', 0) for p in planes.values())
    total_ac = sum(p.get('ac_os', 0) for p in planes.values())
    afiliado = total_importe100 - total_ac

    bonificaciones = abs(pre['bonificaciones'])
    ret_cofa = abs(pre['fdo_prest_colfarma']) + abs(pre['retencion_colegio_art12'])
    deb_os = abs(pre.get('deb_cred_os', 0)) if pre.get('deb_cred_os', 0) < 0 else 0
    cred_os = pre.get('deb_cred_os', 0) if pre.get('deb_cred_os', 0) > 0 else 0
    liq_final = pre['total_liquidacion'] - opf['efvo_up']
    total_pagado = opf['efvo_up'] + liq_final
    dias_prom = (opf['efvo_up']*dias_ant + liq_final*dias_liq) / total_pagado if total_pagado else 0
    periodo = f'{mes}/{anio}'
    planes_activos = {k: v for k, v in planes.items() if v.get('recetas', 0) > 0}

    wb = Workbook(); ws = wb.active; ws.title = 'Reporte'
    setup_ws(ws); header_bg(ws)

    ws.merge_cells('B2:C3'); c(ws,'B2','UNIÓN PERSONAL',bold=True,size=16,color=WHITE,fill=DARK_BLUE,halign='center')
    ws.merge_cells('E2:F3'); c(ws,'E2',periodo,bold=True,size=20,color=WHITE,fill=DARK_BLUE,halign='center')
    c(ws,'H2','RECETAS',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'H3',total_recetas,bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center',num_fmt='#,##0')
    c(ws,'I2','FECHA DE PRESENTACION',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'I3',fecha_pres.strftime('%d/%m/%Y'),bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center')
    c(ws,'K2','DÍAS PROM.',size=9,color='D6E4F0',fill=DARK_BLUE,halign='center')
    c(ws,'K3',round(dias_prom,1),bold=True,size=14,color=WHITE,fill=DARK_BLUE,halign='center',num_fmt='#,##0.0')

    for coord,label,val,clr in [('B5:C5','IMPORTE 100%',total_importe100,MID_BLUE),('E5:F5','A/C UNIÓN PERSONAL',total_ac,MID_BLUE),('H5:I5','AFILIADO',afiliado,MID_BLUE),('K5:L5','TOTAL PAGADO UP',total_pagado,GREEN)]:
        ws.merge_cells(coord); start=coord.split(':')[0]; end=coord.split(':')[1]
        r=int(start[1]); c_letter=start[0]; end_letter=end[0]
        c(ws,f'{c_letter}{r}',label,size=9,color=WHITE,fill=clr,halign='center')
        coord6=f'{c_letter}{r+1}:{end_letter}{r+1}'; ws.merge_cells(coord6)
        ni(ws,f'{c_letter}{r+1}',val); ws[f'{c_letter}{r+1}'].font=Font(bold=True,size=13,color=WHITE); ws[f'{c_letter}{r+1}'].fill=PatternFill('solid',fgColor=clr); ws[f'{c_letter}{r+1}'].alignment=Alignment(horizontal='center',vertical='center')

    for col,col_dias,lbl,dias,val in [('B','C','ANTICIPO',dias_ant,opf['efvo_up']),('E','F','LIQ. FINAL',dias_liq,liq_final)]:
        c(ws,f'{col}8',lbl,size=9,color=WHITE,fill=MID_BLUE,halign='center')
        c(ws,f'{col_dias}8','DIAS DE PAGO',size=9,color=WHITE,fill=MID_BLUE,halign='center')
        n(ws,f'{col}9',val); ws[f'{col}9'].font=Font(bold=True,size=13,color=WHITE); ws[f'{col}9'].fill=PatternFill('solid',fgColor=MID_BLUE); ws[f'{col}9'].alignment=Alignment(horizontal='center',vertical='center')
        c(ws,f'{col_dias}9',dias,bold=True,size=13,color=WHITE,fill=MID_BLUE,halign='center')

    ws.merge_cells('B11:C11'); c(ws,'B11','CARÁTULAS',bold=True,size=11,halign='center')
    ws.merge_cells('E11:F11'); c(ws,'E11','DESCUENTOS',bold=True,size=11,halign='center')
    ws.merge_cells('H11:L11'); c(ws,'H11','PAGOS A FARMACIA',bold=True,size=11,halign='center')

    ws.merge_cells('B12:C12'); c(ws,'B12','COMPOSICIÓN POR PLAN',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'B13','PLAN',bold=True); c(ws,'C13','RECETAS',bold=True,halign='right')
    row = 14
    for plan, datos in planes_activos.items():
        c(ws,f'B{row}',plan); ws[f'C{row}'].value=datos['recetas']; ws[f'C{row}'].alignment=Alignment(horizontal='right',vertical='center'); ws[f'C{row}'].font=Font(size=10); row+=1
    c(ws,f'B{row}','TOTAL',bold=True); ws[f'C{row}'].value=total_recetas; ws[f'C{row}'].alignment=Alignment(horizontal='right',vertical='center'); ws[f'C{row}'].font=Font(bold=True,size=10)
    row_end_car=row; box(ws,11,2,row_end_car,3)

    ws.merge_cells('E12:F12'); c(ws,'E12','BONIFICACIONES',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E13','Total:'); n(ws,'F13',bonificaciones)
    ws.merge_cells('E14:F14'); c(ws,'E14','RETENCIONES',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E15','Fdo Prest. COLFARMA:'); n(ws,'F15',abs(pre['fdo_prest_colfarma']))
    c(ws,'E16','Colegio Art. 12 SU:'); n(ws,'F16',abs(pre['retencion_colegio_art12']))
    c(ws,'E17','TOTAL',bold=True); n(ws,'F17',ret_cofa)
    ws.merge_cells('E18:F18'); c(ws,'E18','DÉB. / CRÉD. OS',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'E19','Débito OS:'); n(ws,'F19',deb_os)
    c(ws,'E20','Crédito OS:'); n(ws,'F20',cred_os)
    box(ws,11,5,20,6)

    ws.merge_cells('H12:L12'); c(ws,'H12','ANTICIPO (OPF)',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'H13','TOTAL',bold=True); n(ws,'I13',opf['efvo_up'])
    c(ws,'H14','Fecha pago'); d(ws,'I14',parse_date(opf['fecha_opf']))
    c(ws,'H15','Comprobante'); ws['I15'].value=opf['nro_comprobante_opf']; ws['I15'].alignment=Alignment(horizontal='right',vertical='center'); ws['I15'].font=Font(size=10)
    ws.merge_cells('H16:L16'); c(ws,'H16','LIQUIDACIÓN',bold=True,size=10,fill=LIGHT_BLUE,halign='center')
    c(ws,'H17','Bruto a pagar antes imp.:'); n(ws,'I17',pre['total_liquidacion'])
    c(ws,'H18','TOTAL',bold=True); n(ws,'I18',liq_final)
    c(ws,'H19','Fecha pago'); d(ws,'I19',parse_date(pago['fecha_pago']))
    c(ws,'H20','Comprobante'); ws['I20'].value=pago['nro_comprobante_pago']; ws['I20'].alignment=Alignment(horizontal='right',vertical='center'); ws['I20'].font=Font(size=10)
    box(ws,11,8,20,12)

    ws2=wb.create_sheet('Resumen'); ws2.sheet_view.showGridLines=False
    style=TableStyleInfo(name='TableStyleMedium2',showFirstColumn=False,showLastColumn=False,showRowStripes=True,showColumnStripes=False)
    h1=['RECETAS','IMPORTE 100%','A/C UP','AFILIADO','%AFL','TOTAL PAGADO','%PAGADO','DIAS PROM','RETENCIONES','%RET','BONIFICACIONES','%BON']
    for i,h in enumerate(h1): ws2.cell(1,i+1,h)
    row2=[total_recetas,total_importe100,total_ac,afiliado,
          afiliado/total_importe100 if total_importe100 else 0,
          total_pagado,total_pagado/total_ac if total_ac else 0,
          round(dias_prom,2),ret_cofa,ret_cofa/total_ac if total_ac else 0,
          bonificaciones,bonificaciones/total_ac if total_ac else 0]
    for i,v in enumerate(row2):
        cell=ws2.cell(2,i+1); cell.value=v
        cell.number_format='0.00%' if isinstance(v,float) and abs(v)<10 else '#,##0.00'
    tbl1=Table(displayName='tbl_unionpersonal',ref='A1:L2'); tbl1.tableStyleInfo=style; ws2.add_table(tbl1)
    h2=['ANTICIPO','%UP ANT','DIAS ANT','LIQ FINAL','%UP LIQ','DIAS LIQ']
    for i,h in enumerate(h2): ws2.cell(4,i+1,h)
    row5=[opf['efvo_up'],opf['efvo_up']/total_pagado if total_pagado else 0,dias_ant,
          liq_final,liq_final/total_pagado if total_pagado else 0,dias_liq]
    for i,v in enumerate(row5):
        cell=ws2.cell(5,i+1); cell.value=v
        cell.number_format='0.00%' if isinstance(v,float) and abs(v)<10 else '#,##0.00'
    tbl2=Table(displayName='tbl_up_desglose',ref='A4:F5'); tbl2.tableStyleInfo=style; ws2.add_table(tbl2)
    for col in 'ABCDEFGHIJKL': ws2.column_dimensions[col].width=20

    buf=io.BytesIO(); wb.save(buf); buf.seek(0)
    return buf

@app.post("/reporte/unionpersonal")
async def reporte_unionpersonal(
    anio: str = Form(...), mes: str = Form(...),
    caratulas: list[UploadFile] = File(...),
    opf: UploadFile = File(...),
    pre: UploadFile = File(...),
    pago: UploadFile = File(...)
):
    opf_bytes = await opf.read()
    pre_bytes = await pre.read()
    pago_bytes = await pago.read()

    planes_data = {
        'PLANES VARIOS': {'recetas':0,'importe100':0.0,'ac_os':0.0},
        'DECLARACIÓN DE DISPENSA': {'recetas':0,'importe100':0.0,'ac_os':0.0},
    }

    fecha_cierre_up = None
    for car_file in caratulas:
        cb = await car_file.read()
        car_data = parse_json(ask_claude(
            pdf_to_content(cb, 'CARÁTULA UNIÓN PERSONAL') + [{"type":"text","text":"Identificar el plan: Planes Varios o Declaracion de dispensa. La fecha_cierre es la \"Fecha de generación\", NO la \"Fecha de Proceso\". Extraé: {\"plan\":\"nombre del plan\",\"fecha_cierre\":\"DD/MM/YYYY\",\"nro_recetas\":0,\"importe_total\":0.0,\"ac_os\":0.0}"}],
            SYSTEM_JSON))
        if not fecha_cierre_up: fecha_cierre_up = car_data.get('fecha_cierre')
        plan_name = car_data.get('plan','').upper()
        key = 'DECLARACIÓN DE DISPENSA' if 'DISPENSA' in plan_name else 'PLANES VARIOS'
        planes_data[key]['recetas'] += car_data.get('nro_recetas',0)
        planes_data[key]['importe100'] += car_data.get('importe_total',0.0)
        planes_data[key]['ac_os'] += car_data.get('ac_os',0.0)

    opf_data = parse_json(ask_claude(
        pdf_to_content(opf_bytes, 'OPF UNIÓN PERSONAL') + [{"type":"text","text":"Buscar línea UNION PERSONAL (SIFAR) con descripción que empiece con Efvo. La fecha_opf es la Fecha del encabezado. El nro_comprobante_opf es el Comprobante del encabezado. Extraé: {\"efvo_up\":0.0,\"fecha_opf\":\"DD/MM/YYYY\",\"nro_comprobante_opf\":\"\"}"}],
        SYSTEM_JSON))

    pre_data = parse_json(ask_claude(
        pdf_to_content(pre_bytes, 'PRE UNIÓN PERSONAL') + [{"type":"text","text":"Extraé: {\"fecha_presentacion\":\"DD/MM/YYYY\",\"nro_comprobante\":0,\"deb_cred_os\":0.0,\"bonificaciones\":0.0,\"fdo_prest_colfarma\":0.0,\"retencion_colegio_art12\":0.0,\"total_liquidacion\":0.0}. deb_cred_os = DEB/CRED DE OBRA SOCIAL (negativo si es débito, 0 si no aparece). bonificaciones=BONIFICACIONES, fdo_prest_colfarma=FDO PREST COLFARMA, total_liquidacion=Total liquidación."}],
        SYSTEM_JSON))

    pago_data = parse_json(ask_claude(
        pdf_to_content(pago_bytes, 'PAGO FINAL UNIÓN PERSONAL') + [{"type":"text","text":f"La fecha_pago es la Fecha del encabezado. El nro_comprobante_pago es el Comprobante del encabezado. Buscar línea UNION PERSONAL con liquidación nro {pre_data.get('nro_comprobante','')}. Extraé: {{\"fecha_pago\":\"DD/MM/YYYY\",\"nro_comprobante_pago\":\"\"}}"}],
        SYSTEM_JSON))

    buf = build_unionpersonal_excel(
        {'planes': planes_data, 'opf': opf_data, 'pre': pre_data, 'pago': pago_data, 'fecha_cierre': fecha_cierre_up},
        mes, anio[-2:]
    )
    filename = f"{anio[-2:]}.{mes} - Reporte Union Personal.xlsx"
    return StreamingResponse(buf, media_type='application/vnd.openxmlformats-officedocument.spreadsheetml.sheet',
                             headers={'Content-Disposition': f'attachment; filename="{filename}"'})

# ── BATCH ─────────────────────────────────────────────────────────────────────

import zipfile
import tempfile

def find_file(files, *keywords):
    """Busca un archivo que contenga alguna de las keywords en el nombre (case insensitive)"""
    for kw in keywords:
        for f in files:
            if kw.lower() in os.path.basename(f).lower():
                return f
    return None

def find_files(files, *keywords):
    """Busca todos los archivos que contengan alguna de las keywords"""
    result = []
    for kw in keywords:
        for f in files:
            if kw.lower() in os.path.basename(f).lower():
                if f not in result:
                    result.append(f)
    return result

def read_file(path):
    with open(path, 'rb') as f:
        return f.read()

@app.post("/batch/pami")
async def batch_pami(zip_file: UploadFile = File(...)):
    zip_bytes = await zip_file.read()
    output_zip = io.BytesIO()

    with tempfile.TemporaryDirectory() as tmpdir:
        # Extraer ZIP
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            z.extractall(tmpdir)

        reportes = []
        errores = []

        # Recorrer estructura: año/mes/quincena
        for root, dirs, files in os.walk(tmpdir):
            # Detectar si es carpeta de quincena (contiene 1Q o 2Q en el path)
            rel = os.path.relpath(root, tmpdir)
            parts = rel.replace('\\', '/').split('/')
            
            # Buscar nivel de quincena: tiene que tener archivos directamente
            if not any('1Q' in p or '2Q' in p for p in parts):
                continue
            
            all_files = [os.path.join(root, f) for f in os.listdir(root) if os.path.isfile(os.path.join(root, f))]
            
            # Buscar subcarpeta de liquidacion
            liq_dir = None
            for d in os.listdir(root):
                full_d = os.path.join(root, d)
                if os.path.isdir(full_d) and 'liquidac' in d.lower():
                    liq_dir = full_d
                    break
            
            if not liq_dir:
                continue

            liq_files = [os.path.join(liq_dir, f) for f in os.listdir(liq_dir)]
            
            caratula = find_file(all_files, 'caratula', 'carátula')
            nr_file = find_file(all_files, 'notas', 'recupero', '.xls')
            opf_file = find_file(liq_files, 'opf')
            pre_file = find_file(liq_files, 'pre')
            pago_file = find_file(liq_files, 'pago')

            if not all([caratula, nr_file, opf_file, pre_file, pago_file]):
                errores.append(f"Faltan archivos en: {rel}")
                continue

            try:
                # Extraer quincena y mes/año del path
                quincena = next((p for p in parts if '1Q' in p or '2Q' in p), None)
                quincena_val = '1Q' if quincena and '1Q' in quincena else '2Q'
                
                # Buscar mes y año en el path
                mes_part = next((p for p in parts if any(m in p for m in ['Enero','Febrero','Marzo','Abril','Mayo','Junio','Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre'])), None)
                anio_part = next((p for p in parts if p.isdigit() and len(p) == 4), None)
                
                if not mes_part or not anio_part:
                    errores.append(f"No se pudo determinar mes/año en: {rel}")
                    continue

                meses_map = {'Enero':'01','Febrero':'02','Marzo':'03','Abril':'04','Mayo':'05','Junio':'06','Julio':'07','Agosto':'08','Septiembre':'09','Octubre':'10','Noviembre':'11','Diciembre':'12'}
                mes_num = next((v for k,v in meses_map.items() if k in mes_part), None)
                anio = anio_part

                car_bytes = read_file(caratula)
                opf_bytes = read_file(opf_file)
                pre_bytes = read_file(pre_file)
                pago_bytes = read_file(pago_file)
                nr_bytes = read_file(nr_file)
                nr_filename = os.path.basename(nr_file)

                periodo = f'{quincena_val} mes {mes_num} año {anio}'

                car_data = parse_json(ask_claude(
                    pdf_to_content(car_bytes, 'CARÁTULA PAMI') + [{"type":"text","text":f"Período: {periodo}. Extraé: {{\"fecha_cierre\":\"DD/MM/YYYY\",\"nro_recetas\":0,\"total_pvp\":0.0,\"total_pvp_pami\":0.0,\"importe_bruto_convenio\":0.0}}"}],
                    SYSTEM_JSON))
                opf_data = parse_json(ask_claude(
                    pdf_to_content(opf_bytes, 'OPF PAMI') + [{"type":"text","text":f"Período: {periodo}. Buscar línea Efvo.PAMI. Extraé: {{\"efvo_pami\":0.0,\"fecha_opf\":\"DD/MM/YYYY\",\"nro_comprobante_opf\":0}}"}],
                    SYSTEM_JSON))
                pre_data = parse_json(ask_claude(
                    pdf_to_content(pre_bytes, 'PRE PAMI') + [{"type":"text","text":"Extraé: {\"deb_cred_os\":0.0,\"bonif_tiras\":0.0,\"bonif_ambulatorio\":0.0,\"bonif_insulinas\":0.0,\"ret_gtos_adm_cofa\":0.0,\"efectivo_drogueria\":0.0,\"fdo_prest_colfarma\":0.0,\"nota_cred_ambulatorio\":0.0,\"nota_cred_insulina\":0.0,\"nota_cred_tiras\":0.0,\"retencion_colegio_art12\":0.0,\"total_liquidacion\":0.0}. Todos los valores deben ser positivos (sin signo negativo). efectivo_drogueria = valor absoluto de EFECTIVO DROGUERIA SALDO."}],
                    SYSTEM_JSON))
                pago_data = parse_json(ask_claude(
                    pdf_to_content(pago_bytes, 'PAGO FINAL PAMI') + [{"type":"text","text":"Buscar línea PAMI. Extraé: {\"fecha_pago\":\"DD/MM/YYYY\",\"nro_comprobante_pago\":0}"}],
                    SYSTEM_JSON))
                nr_text = xls_to_text(nr_bytes, nr_filename)
                nr_data = parse_json(ask_claude(
                    [{"type":"text","text":f"NOTAS DE RECUPERO PAMI:\n{nr_text}\n\nLa tabla tiene columnas: TipoCte e ImporteCpte (entre otras). Sumá TODOS los valores de ImporteCpte agrupando por TipoCte (CCF, CCFD, NAF, NRFD, EfSa). Usá la columna ImporteCpte, NO ImporteCo. Fecha de Impresion de primera fila CCF o NAF = fecha_nr. Fecha de primera fila EfSa = fecha_efsa. Extraé: {{\"nr_ccf\":0.0,\"nr_ccfd\":0.0,\"nr_naf\":0.0,\"nr_nrfd\":0.0,\"nr_efsa\":0.0,\"fecha_nr\":\"DD/MM/YYYY\",\"fecha_efsa\":\"DD/MM/YYYY\"}}"}],
                    SYSTEM_JSON))

                buf = build_pami_excel(
                    {'caratula': car_data, 'opf': opf_data, 'pre': pre_data, 'pago': pago_data, 'nr': nr_data},
                    quincena_val, mes_num, anio[-2:]
                )
                filename = f"{anio[-2:]}.{mes_num}.{quincena_val} - Reporte.xlsx"
                reportes.append((filename, buf.getvalue()))

            except Exception as e:
                errores.append(f"Error en {rel}: {str(e)}")

        if not reportes:
            raise HTTPException(status_code=400, detail=f"No se generaron reportes. Errores: {errores}")

        # Crear ZIP de salida
        with zipfile.ZipFile(output_zip, 'w') as zout:
            for filename, data in reportes:
                zout.writestr(filename, data)
            if errores:
                zout.writestr("errores.txt", "\n".join(errores))

    output_zip.seek(0)
    return StreamingResponse(output_zip, media_type='application/zip',
                             headers={'Content-Disposition': 'attachment; filename="Reportes_PAMI.zip"'})

@app.post("/batch/ioma")
async def batch_ioma(zip_file: UploadFile = File(...)):
    zip_bytes = await zip_file.read()
    output_zip = io.BytesIO()

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            z.extractall(tmpdir)

        reportes = []; errores = []
        meses_map = {'Enero':'01','Febrero':'02','Marzo':'03','Abril':'04','Mayo':'05','Junio':'06','Julio':'07','Agosto':'08','Septiembre':'09','Octubre':'10','Noviembre':'11','Diciembre':'12'}

        for root, dirs, files in os.walk(tmpdir):
            rel = os.path.relpath(root, tmpdir).replace('\\','/')
            parts = rel.split('/')
            # Nivel de mes: contiene nombre de mes y tiene subcarpetas de caratulas/liquidaciones
            if not any(m in rel for m in meses_map.keys()): continue
            if not any(os.path.isdir(os.path.join(root, d)) for d in os.listdir(root)): continue

            all_files = [os.path.join(root, f) for f in os.listdir(root) if os.path.isfile(os.path.join(root, f))]
            subdirs = [os.path.join(root, d) for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]

            car_dir = next((d for d in subdirs if 'caratula' in os.path.basename(d).lower()), None)
            liq_dir = next((d for d in subdirs if 'liquidac' in os.path.basename(d).lower()), None)
            nr_file = find_file(all_files, 'notas', 'recupero', '.xls')

            if not liq_dir: continue

            car_files = sorted([os.path.join(car_dir, f) for f in os.listdir(car_dir)]) if car_dir else []
            liq_files = [os.path.join(liq_dir, f) for f in os.listdir(liq_dir)]

            opf_file = find_file(liq_files, 'opf')
            pre_file = find_file(liq_files, 'pre')
            pago_file = find_file(liq_files, 'pago')

            if not all([car_files, opf_file, pre_file, pago_file]):
                errores.append(f"Faltan archivos en: {rel}"); continue

            anio_part = next((p for p in parts if p.isdigit() and len(p)==4), None)
            mes_part = next((p for p in parts if any(m in p for m in meses_map.keys())), None)
            if not anio_part or not mes_part:
                errores.append(f"No se pudo determinar mes/año en: {rel}"); continue

            mes_num = next((v for k,v in meses_map.items() if k in mes_part), None)
            anio = anio_part

            try:
                agudo_bytes = read_file(car_files[0])
                agudo_data = parse_json(ask_claude(
                    pdf_to_content(agudo_bytes, 'AGUDO/CRÓNICO IOMA') + [{"type":"text","text":"Buscar RX ON LINE/TOTALIDAD DE LAS RECETAS. Si es un Resumen de Facturación del Colegio, la fecha_cierre es la \"Fecha de Proceso\". Si es una Carátula On-Line, la fecha_cierre es la \"Fecha de generación\". Extraé: {\"fecha_cierre\":\"DD/MM/YYYY\",\"recetas\":0,\"importe100\":0.0,\"ac_instituto\":0.0}"}],
                    SYSTEM_JSON))

                planes_data = {
                    'AGUDO/CRÓNICO': {'recetas':agudo_data['recetas'],'importe100':agudo_data['importe100'],'ac_instituto':agudo_data['ac_instituto']},
                    'RECURSOS DE AMPARO': {'recetas':0,'importe100':0,'ac_instituto':0},
                    'RESOLUCIÓN DE DIRECTORIO': {'recetas':0,'importe100':0,'ac_instituto':0},
                    'MAMI': {'recetas':0,'importe100':0,'ac_instituto':0},
                    'MAYOR COBERTURA': {'recetas':0,'importe100':0,'ac_instituto':0},
                    'VACUNAS': {'recetas':0,'importe100':0,'ac_instituto':0},
                }

                for cb_path in car_files[1:]:
                    cb = read_file(cb_path)
                    # Detectar tipo
                    tipo_data = parse_json(ask_claude(
                        pdf_to_content(cb, 'DOCUMENTO PLAN IOMA') + [{"type":"text","text":"Este documento es un Resumen de Facturación del Colegio con tabla de múltiples planes, O es una carátula individual de un solo plan IOMA. Respondé SOLO: {\"tipo\":\"resumen_colegio\"} o {\"tipo\":\"caratula_individual\"}"}],
                        SYSTEM_JSON))
                    if tipo_data.get('tipo') == 'resumen_colegio':
                        planes_lista = parse_json(ask_claude(
                            pdf_to_content(cb, 'RESUMEN COLEGIO IOMA') + [{"type":"text","text":"Extraé cada fila de la tabla de planes. Para cada plan: importe100=Imp.Total, ac_instituto=Imp.Os. Respondé: {\"planes\":[{\"plan\":\"nombre del convenio/plan\",\"recetas\":0,\"importe100\":0.0,\"ac_instituto\":0.0}]}"}],
                            SYSTEM_JSON))
                        for p in planes_lista.get('planes', []):
                            plan_name = p.get('plan','').upper()
                            entry = {'recetas':p['recetas'],'importe100':p['importe100'],'ac_instituto':p['ac_instituto']}
                            if 'MAMI' in plan_name: planes_data['MAMI'] = entry
                            elif 'MAYOR' in plan_name: planes_data['MAYOR COBERTURA'] = entry
                            elif 'AMPARO' in plan_name: planes_data['RECURSOS DE AMPARO'] = entry
                            elif 'DIRECTORIO' in plan_name: planes_data['RESOLUCIÓN DE DIRECTORIO'] = entry
                            elif 'VACUNA' in plan_name: planes_data['VACUNAS'] = entry
                    else:
                        pd_data = parse_json(ask_claude(
                            pdf_to_content(cb, 'CARÁTULA PLAN IOMA') + [{"type":"text","text":"Leer Convenio/Plan para identificar el plan. Extraé: {\"plan\":\"nombre del plan\",\"recetas\":0,\"importe100\":0.0,\"ac_instituto\":0.0}"}],
                            SYSTEM_JSON))
                        plan_name = pd_data.get('plan','').upper()
                        entry = {'recetas':pd_data['recetas'],'importe100':pd_data['importe100'],'ac_instituto':pd_data['ac_instituto']}
                        if 'MAMI' in plan_name: planes_data['MAMI'] = entry
                        elif 'MAYOR' in plan_name: planes_data['MAYOR COBERTURA'] = entry
                        elif 'AMPARO' in plan_name: planes_data['RECURSOS DE AMPARO'] = entry
                        elif 'DIRECTORIO' in plan_name: planes_data['RESOLUCIÓN DE DIRECTORIO'] = entry
                        elif 'VACUNA' in plan_name: planes_data['VACUNAS'] = entry

                opf_data = parse_json(ask_claude(
                    pdf_to_content(read_file(opf_file), 'OPF IOMA') + [{"type":"text","text":"Efvo.IOMA AMBULATORIO = anticipo. Sección RETENCIONES línea RGI = ing_brutos_anticipo. Extraé: {\"efvo_ioma\":0.0,\"fecha_opf\":\"DD/MM/YYYY\",\"nro_comprobante_opf\":0,\"ing_brutos_anticipo\":0.0}"}],
                    SYSTEM_JSON))
                pre_data = parse_json(ask_claude(
                    pdf_to_content(read_file(pre_file), 'PRE IOMA') + [{"type":"text","text":"Extraé: {\"deb_cred_os\":0.0,\"bonificaciones\":0.0,\"fdo_prest_colfarma\":0.0,\"nrf_ant\":0.0,\"nrf_def\":0.0,\"nrf_directas\":0.0,\"retencion_colegio_art12\":0.0,\"total_liquidacion\":0.0}"}],
                    SYSTEM_JSON))
                pago_data = parse_json(ask_claude(
                    pdf_to_content(read_file(pago_file), 'PAGO FINAL IOMA') + [{"type":"text","text":"Sección RETENCIONES línea RGI = ing_brutos_pago. Extraé: {\"fecha_pago\":\"DD/MM/YYYY\",\"nro_comprobante_pago\":0,\"ing_brutos_pago\":0.0}"}],
                    SYSTEM_JSON))

                nr_data = {'nr_por_fecha':[]}
                if nr_file:
                    nr_text = xls_to_text(read_file(nr_file), os.path.basename(nr_file))
                    nr_data = parse_json(ask_claude(
                        [{"type":"text","text":f"NOTAS DE RECUPERO IOMA:\n{nr_text}\n\nSumá NRF y NRFD agrupados por fecha de Impresion. Extraé: {{\"nr_por_fecha\":[{{\"fecha\":\"DD/MM/YYYY\",\"monto\":0.0}}]}}"}],
                        SYSTEM_JSON))

                buf = build_ioma_excel(
                    {'planes':planes_data,'opf':opf_data,'pre':pre_data,'pago':pago_data,'nr':nr_data,'fecha_cierre':agudo_data['fecha_cierre']},
                    mes_num, anio[-2:])
                reportes.append((f"{anio[-2:]}.{mes_num} - Reporte IOMA.xlsx", buf.getvalue()))
            except Exception as e:
                errores.append(f"Error en {rel}: {str(e)}")

        if not reportes:
            raise HTTPException(status_code=400, detail=f"No se generaron reportes. Errores: {errores}")
        with zipfile.ZipFile(output_zip, 'w') as zout:
            for filename, data in reportes:
                zout.writestr(filename, data)
            if errores:
                zout.writestr("errores.txt", "\n".join(errores))

    output_zip.seek(0)
    return StreamingResponse(output_zip, media_type='application/zip',
                             headers={'Content-Disposition': 'attachment; filename="Reportes_IOMA.zip"'})


@app.post("/batch/osde")
async def batch_osde(zip_file: UploadFile = File(...)):
    zip_bytes = await zip_file.read()
    output_zip = io.BytesIO()

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            z.extractall(tmpdir)

        reportes = []; errores = []
        meses_map = {'Enero':'01','Febrero':'02','Marzo':'03','Abril':'04','Mayo':'05','Junio':'06','Julio':'07','Agosto':'08','Septiembre':'09','Octubre':'10','Noviembre':'11','Diciembre':'12'}

        for root, dirs, files in os.walk(tmpdir):
            rel = os.path.relpath(root, tmpdir).replace('\\','/')
            parts = rel.split('/')
            if not any(m in rel for m in meses_map.keys()): continue
            if not any(os.path.isdir(os.path.join(root, d)) for d in os.listdir(root)): continue

            all_files = [os.path.join(root, f) for f in os.listdir(root) if os.path.isfile(os.path.join(root, f))]
            subdirs = [os.path.join(root, d) for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]
            liq_dir = next((d for d in subdirs if 'liquidac' in os.path.basename(d).lower()), None)
            if not liq_dir: continue

            liq_files = [os.path.join(liq_dir, f) for f in os.listdir(liq_dir)]
            nr_file = find_file(all_files, 'notas', 'recupero')
            car_file = find_file(liq_files, 'cierre', 'caratula', 'carátula')
            pre_file = find_file(liq_files, 'pre')
            pago_file = find_file(liq_files, 'pago')

            if not all([car_file, pre_file, pago_file]):
                errores.append(f"Faltan archivos en: {rel}"); continue

            anio_part = next((p for p in parts if p.isdigit() and len(p)==4), None)
            mes_part = next((p for p in parts if any(m in p for m in meses_map.keys())), None)
            if not anio_part or not mes_part:
                errores.append(f"No se pudo determinar mes/año en: {rel}"); continue
            mes_num = next((v for k,v in meses_map.items() if k in mes_part), None)
            anio = anio_part

            try:
                car_data = parse_json(ask_claude(
                    pdf_to_content(read_file(car_file), 'CARÁTULA OSDE') + [{"type":"text","text":"Extraé: {\"fecha_cierre\":\"DD/MM/YYYY\",\"nro_recetas\":0,\"importe_total\":0.0,\"afiliado\":0.0,\"a_cargo_osde\":0.0,\"bonificacion\":0.0,\"total_verificar\":0.0}"}],
                    SYSTEM_JSON))
                pre_data = parse_json(ask_claude(
                    pdf_to_content(read_file(pre_file), 'PRE OSDE') + [{"type":"text","text":"La tabla tiene columnas: Concepto, Base Cálculo, Créditos, Débitos. REGLAS: 1) Para retencion_fdo_res, ret_col_art12 y notas_credito tomá SIEMPRE el valor de la columna Débitos, NO la Base de Cálculo. 2) Para ajuste_facturacion: puede haber varias líneas de Ajuste Facturación. Identificá pares que se cancelan entre sí (mismo monto, uno en Débitos y otro en Créditos) y excluílos. El ajuste_facturacion es el monto de la línea que NO tiene contraparte que la cancele (si es débito es positivo, si es crédito es negativo). Si no hay ajuste real, ajuste_facturacion=0. 3) neto_cobrar = fila Neto a Cobrar columna Créditos. Extraé: {\"nro_liquidacion\":0,\"ajuste_facturacion\":0.0,\"retencion_fdo_res\":0.0,\"ret_col_art12\":0.0,\"notas_credito\":0.0,\"neto_cobrar\":0.0}"}],
                    SYSTEM_JSON))
                pago_data = parse_json(ask_claude(
                    pdf_to_content(read_file(pago_file), 'PAGO FINAL OSDE') + [{"type":"text","text":f"La fecha_pago es la fecha del ENCABEZADO del documento (campo Fecha:), NO la fecha de presentación de la tabla. El nro_comprobante_pago es el número de Orden de Pago del encabezado. Confirmar que existe línea OSDE con liquidación nro {pre_data.get('nro_liquidacion','')}. Extraé: {{\"fecha_pago\":\"DD/MM/YYYY\",\"nro_comprobante_pago\":0}}"}],
                    SYSTEM_JSON))
                nr_data = {'nr_monto':0.0,'nr_fecha':'01/01/2000'}
                if nr_file:
                    nr_data = parse_json(ask_claude(
                        pdf_to_content(read_file(nr_file), 'NOTA DE CRÉDITO DEL SUD') + [{"type":"text","text":"Tomar el TOTAL del documento. Extraé: {\"nr_monto\":0.0,\"nr_fecha\":\"DD/MM/YYYY\"}"}],
                        SYSTEM_JSON))

                buf = build_osde_excel({'caratula':car_data,'pre':pre_data,'pago':pago_data,'nr':nr_data}, mes_num, anio[-2:])
                reportes.append((f"{anio[-2:]}.{mes_num} - Reporte OSDE.xlsx", buf.getvalue()))
            except Exception as e:
                errores.append(f"Error en {rel}: {str(e)}")

        if not reportes:
            raise HTTPException(status_code=400, detail=f"No se generaron reportes. Errores: {errores}")
        with zipfile.ZipFile(output_zip, 'w') as zout:
            for filename, data in reportes:
                zout.writestr(filename, data)
            if errores:
                zout.writestr("errores.txt", "\n".join(errores))

    output_zip.seek(0)
    return StreamingResponse(output_zip, media_type='application/zip',
                             headers={'Content-Disposition': 'attachment; filename="Reportes_OSDE.zip"'})


@app.post("/batch/ospecon")
async def batch_ospecon(zip_file: UploadFile = File(...)):
    zip_bytes = await zip_file.read()
    output_zip = io.BytesIO()

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            z.extractall(tmpdir)

        reportes = []; errores = []
        meses_map = {'Enero':'01','Febrero':'02','Marzo':'03','Abril':'04','Mayo':'05','Junio':'06','Julio':'07','Agosto':'08','Septiembre':'09','Octubre':'10','Noviembre':'11','Diciembre':'12'}

        for root, dirs, files in os.walk(tmpdir):
            rel = os.path.relpath(root, tmpdir).replace('\\','/')
            parts = rel.split('/')
            if not any(m in rel for m in meses_map.keys()): continue
            if not any(os.path.isdir(os.path.join(root, d)) for d in os.listdir(root)): continue

            all_files = [os.path.join(root, f) for f in os.listdir(root) if os.path.isfile(os.path.join(root, f))]
            subdirs = [os.path.join(root, d) for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]
            liq_dir = next((d for d in subdirs if 'liquidac' in os.path.basename(d).lower()), None)
            if not liq_dir: continue

            car_files = [f for f in all_files if 'caratula' in os.path.basename(f).lower() or 'carátula' in os.path.basename(f).lower()]
            liq_files = [os.path.join(liq_dir, f) for f in os.listdir(liq_dir)]
            pre_file = find_file(liq_files, 'pre')
            pago_file = find_file(liq_files, 'pago')

            if not all([car_files, pre_file, pago_file]):
                errores.append(f"Faltan archivos en: {rel}"); continue

            anio_part = next((p for p in parts if p.isdigit() and len(p)==4), None)
            mes_part = next((p for p in parts if any(m in p for m in meses_map.keys())), None)
            if not anio_part or not mes_part:
                errores.append(f"No se pudo determinar mes/año en: {rel}"); continue
            mes_num = next((v for k,v in meses_map.items() if k in mes_part), None)
            anio = anio_part

            try:
                car_data = parse_json(ask_claude(
                    pdf_to_content(read_file(car_files[0]), 'CARÁTULA OSPECON') + [{"type":"text","text":"La fecha_cierre es la \"Fecha de generación\", NO la \"Fecha de Proceso\". Extraé: {\"fecha_cierre\":\"DD/MM/YYYY\",\"nro_recetas\":0,\"importe_total\":0.0,\"ac_os\":0.0}"}],
                    SYSTEM_JSON))
                pre_data = parse_json(ask_claude(
                    pdf_to_content(read_file(pre_file), 'PRE OSPECON') + [{"type":"text","text":"Columnas: Concepto, Base Cálculo, Créditos, Débitos. Para bonificacion, retencion_fdo_res y ret_col_art12 tomá SIEMPRE la columna Débitos. Para ajuste_facturacion: identificá pares que se cancelan entre sí y excluílos, el ajuste_facturacion es el monto de la línea sin contraparte (positivo si débito, negativo si crédito), 0 si no hay. neto_cobrar = fila Neto a Cobrar columna Créditos. Extraé: {\"nro_liquidacion\":0,\"ajuste_facturacion\":0.0,\"bonificacion\":0.0,\"retencion_fdo_res\":0.0,\"ret_col_art12\":0.0,\"neto_cobrar\":0.0}"}],
                    SYSTEM_JSON))
                pago_data = parse_json(ask_claude(
                    pdf_to_content(read_file(pago_file), 'PAGO FINAL OSPECON') + [{"type":"text","text":f"La fecha_pago es la fecha del ENCABEZADO del documento (campo Fecha:). El nro_comprobante_pago es el número de Orden de Pago del encabezado. Confirmar línea OSPECON con liquidación nro {pre_data.get('nro_liquidacion','')}. Extraé: {{\"fecha_pago\":\"DD/MM/YYYY\",\"nro_comprobante_pago\":0}}"}],
                    SYSTEM_JSON))

                buf = build_ospecon_excel({'caratula':car_data,'pre':pre_data,'pago':pago_data}, mes_num, anio[-2:])
                reportes.append((f"{anio[-2:]}.{mes_num} - Reporte OSPECON.xlsx", buf.getvalue()))
            except Exception as e:
                errores.append(f"Error en {rel}: {str(e)}")

        if not reportes:
            raise HTTPException(status_code=400, detail=f"No se generaron reportes. Errores: {errores}")
        with zipfile.ZipFile(output_zip, 'w') as zout:
            for filename, data in reportes:
                zout.writestr(filename, data)
            if errores:
                zout.writestr("errores.txt", "\n".join(errores))

    output_zip.seek(0)
    return StreamingResponse(output_zip, media_type='application/zip',
                             headers={'Content-Disposition': 'attachment; filename="Reportes_OSPECON.zip"'})


@app.post("/batch/osprera")
async def batch_osprera(zip_file: UploadFile = File(...)):
    zip_bytes = await zip_file.read()
    output_zip = io.BytesIO()

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            z.extractall(tmpdir)

        reportes = []; errores = []
        meses_map = {'Enero':'01','Febrero':'02','Marzo':'03','Abril':'04','Mayo':'05','Junio':'06','Julio':'07','Agosto':'08','Septiembre':'09','Octubre':'10','Noviembre':'11','Diciembre':'12'}

        for root, dirs, files in os.walk(tmpdir):
            rel = os.path.relpath(root, tmpdir).replace('\\','/')
            parts = rel.split('/')
            if not any(m in rel for m in meses_map.keys()): continue

            all_files = [os.path.join(root, f) for f in os.listdir(root) if os.path.isfile(os.path.join(root, f))]
            subdirs = [os.path.join(root, d) for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]

            anio_part = next((p for p in parts if p.isdigit() and len(p)==4), None)
            mes_part = next((p for p in parts if any(m in p for m in meses_map.keys())), None)
            if not anio_part or not mes_part: continue
            mes_num = next((v for k,v in meses_map.items() if k in mes_part), None)
            anio = anio_part

            # Detectar si tiene quincena
            con_quincena = any('1Q' in p or '2Q' in p for p in parts)
            quincena_val = next((p for p in parts if '1Q' in p or '2Q' in p), None)
            if quincena_val:
                quincena_val = '1Q' if '1Q' in quincena_val else '2Q'

            liq_dir = next((d for d in subdirs if 'liquidac' in os.path.basename(d).lower()), None)
            car_dir = next((d for d in subdirs if 'caratula' in os.path.basename(d).lower() or 'carátula' in os.path.basename(d).lower()), None)

            if con_quincena:
                # Archivos sueltos en la carpeta
                car_files = [f for f in all_files if any(x in os.path.basename(f).lower() for x in ['caratula','carátula'])]
                nr_file = find_file(all_files, 'notas', 'recupero', '.xls')
            else:
                car_files = sorted([os.path.join(car_dir, f) for f in os.listdir(car_dir)]) if car_dir else []
                nr_file = None

            if not liq_dir or not car_files: continue
            liq_files = [os.path.join(liq_dir, f) for f in os.listdir(liq_dir)]
            pre_file = find_file(liq_files, 'pre')
            pago_file = find_file(liq_files, 'pago')
            if not all([pre_file, pago_file]): continue

            try:
                planes_data = {'GENERAL':{'recetas':0,'importe100':0.0,'ac_os':0.0},'TRATAMIENTO PROLONGADO':{'recetas':0,'importe100':0.0,'ac_os':0.0},'MONOTRIBUTISTAS':{'recetas':0,'importe100':0.0,'ac_os':0.0},'RURAL':{'recetas':0,'importe100':0.0,'ac_os':0.0},'DECLARACIÓN DE DISPENSA':{'recetas':0,'importe100':0.0,'ac_os':0.0}}
                fecha_cierre = None
                for cp in car_files:
                    car_data = parse_json(ask_claude(
                        pdf_to_content(read_file(cp), 'CARÁTULA OSPRERA') + [{"type":"text","text":"Identificar el plan exacto leyendo el campo Convenio/Plan. La fecha_cierre es la \"Fecha de generación\", NO la \"Fecha de Proceso\". Extraé: {\"plan\":\"nombre completo del convenio/plan\",\"fecha_cierre\":\"DD/MM/YYYY\",\"nro_recetas\":0,\"importe_total\":0.0,\"ac_os\":0.0}"}],
                        SYSTEM_JSON))
                    if not fecha_cierre: fecha_cierre = car_data.get('fecha_cierre')
                    pn = car_data.get('plan','').upper()
                    k = 'MONOTRIBUTISTAS' if 'MONOT' in pn else 'RURAL' if 'RURAL' in pn else 'PROLONGADO' if 'PROLONGADO' in pn else 'DECLARACIÓN DE DISPENSA' if 'DISPENSA' in pn else 'GENERAL'
                    planes_data[k]['recetas'] += car_data.get('nro_recetas',0)
                    planes_data[k]['importe100'] += car_data.get('importe_total',0.0)
                    planes_data[k]['ac_os'] += car_data.get('ac_os',0.0)

                nr_data = None
                if con_quincena and nr_file:
                    nr_text = xls_to_text(read_file(nr_file), os.path.basename(nr_file))
                    nr_data = parse_json(ask_claude(
                        [{"type":"text","text":f"NOTAS DE RECUPERO OSPRERA:\n{nr_text}\n\nSumá NRF y NRFD agrupados por fecha. Extraé: {{\"nr_por_fecha\":[{{\"fecha\":\"DD/MM/YYYY\",\"monto\":0.0}}]}}"}],
                        SYSTEM_JSON))

                pre_data = parse_json(ask_claude(
                    pdf_to_content(read_file(pre_file), 'PRE OSPRERA') + [{"type":"text","text":"Extraé: {\"fecha_presentacion\":\"DD/MM/YYYY\",\"nro_comprobante\":0,\"deb_cred_os\":0.0,\"bonificaciones\":0.0,\"notas_credito\":0.0,\"fdo_prest_colfarma\":0.0,\"retencion_colegio_art12\":0.0,\"total_liquidacion\":0.0}. deb_cred_os = DEB/CRED DE OBRA SOCIAL (negativo si es débito). bonificaciones = BONIFICACIONES. notas_credito = NOTAS DE CREDITO. fdo_prest_colfarma = FDO PREST COLFARMA. total_liquidacion = Total liquidación."}],
                    SYSTEM_JSON))
                pago_data = parse_json(ask_claude(
                    pdf_to_content(read_file(pago_file), 'PAGO FINAL OSPRERA') + [{"type":"text","text":f"La fecha_pago es la Fecha del encabezado del documento. El nro_comprobante_pago es el número de Comprobante del encabezado. Confirmar que existe línea OSPRERA con comprobante {pre_data.get('nro_comprobante','')}. Extraé: {{\"fecha_pago\":\"DD/MM/YYYY\",\"nro_comprobante_pago\":\"\"}}"}],
                    SYSTEM_JSON))

                q_str = f".{quincena_val}" if con_quincena and quincena_val else ""
                buf = build_osprera_excel(
                    {'planes':planes_data,'pre':pre_data,'pago':pago_data,'fecha_cierre':fecha_cierre,'opf':None,'nr':nr_data,'con_quincena':con_quincena,'quincena':quincena_val},
                    mes_num, anio[-2:])
                reportes.append((f"{anio[-2:]}.{mes_num}{q_str} - Reporte OSPRERA.xlsx", buf.getvalue()))
            except Exception as e:
                errores.append(f"Error en {rel}: {str(e)}")

        if not reportes:
            raise HTTPException(status_code=400, detail=f"No se generaron reportes. Errores: {errores}")
        with zipfile.ZipFile(output_zip, 'w') as zout:
            for filename, data in reportes:
                zout.writestr(filename, data)
            if errores:
                zout.writestr("errores.txt", "\n".join(errores))

    output_zip.seek(0)
    return StreamingResponse(output_zip, media_type='application/zip',
                             headers={'Content-Disposition': 'attachment; filename="Reportes_OSPRERA.zip"'})


@app.post("/batch/unionpersonal")
async def batch_unionpersonal(zip_file: UploadFile = File(...)):
    zip_bytes = await zip_file.read()
    output_zip = io.BytesIO()

    with tempfile.TemporaryDirectory() as tmpdir:
        with zipfile.ZipFile(io.BytesIO(zip_bytes)) as z:
            z.extractall(tmpdir)

        reportes = []; errores = []
        meses_map = {'Enero':'01','Febrero':'02','Marzo':'03','Abril':'04','Mayo':'05','Junio':'06','Julio':'07','Agosto':'08','Septiembre':'09','Octubre':'10','Noviembre':'11','Diciembre':'12'}

        for root, dirs, files in os.walk(tmpdir):
            rel = os.path.relpath(root, tmpdir).replace('\\','/')
            parts = rel.split('/')
            if not any(m in rel for m in meses_map.keys()): continue
            if not any(os.path.isdir(os.path.join(root, d)) for d in os.listdir(root)): continue

            subdirs = [os.path.join(root, d) for d in os.listdir(root) if os.path.isdir(os.path.join(root, d))]
            car_dir = next((d for d in subdirs if 'caratula' in os.path.basename(d).lower() or 'carátula' in os.path.basename(d).lower()), None)
            liq_dir = next((d for d in subdirs if 'liquidac' in os.path.basename(d).lower()), None)
            if not car_dir or not liq_dir: continue

            car_files = sorted([os.path.join(car_dir, f) for f in os.listdir(car_dir) if f.endswith('.pdf')])
            liq_files = [os.path.join(liq_dir, f) for f in os.listdir(liq_dir)]
            opf_file = find_file(liq_files, 'opf')
            pre_file = find_file(liq_files, 'pre')
            pago_file = find_file(liq_files, 'pago')

            if not all([car_files, opf_file, pre_file, pago_file]):
                errores.append(f"Faltan archivos en: {rel}"); continue

            anio_part = next((p for p in parts if p.isdigit() and len(p)==4), None)
            mes_part = next((p for p in parts if any(m in p for m in meses_map.keys())), None)
            if not anio_part or not mes_part: continue
            mes_num = next((v for k,v in meses_map.items() if k in mes_part), None)
            anio = anio_part

            try:
                planes_data = {'PLANES VARIOS':{'recetas':0,'importe100':0.0,'ac_os':0.0},'DECLARACIÓN DE DISPENSA':{'recetas':0,'importe100':0.0,'ac_os':0.0}}
                fecha_cierre = None
                for cp in car_files:
                    car_data = parse_json(ask_claude(
                        pdf_to_content(read_file(cp), 'CARÁTULA UNIÓN PERSONAL') + [{"type":"text","text":"Identificar el plan: Planes Varios o Declaracion de dispensa. La fecha_cierre es la \"Fecha de generación\", NO la \"Fecha de Proceso\". Extraé: {\"plan\":\"nombre del plan\",\"fecha_cierre\":\"DD/MM/YYYY\",\"nro_recetas\":0,\"importe_total\":0.0,\"ac_os\":0.0}"}],
                        SYSTEM_JSON))
                    if not fecha_cierre: fecha_cierre = car_data.get('fecha_cierre')
                    key = 'DECLARACIÓN DE DISPENSA' if 'DISPENSA' in car_data.get('plan','').upper() else 'PLANES VARIOS'
                    planes_data[key]['recetas'] += car_data.get('nro_recetas',0)
                    planes_data[key]['importe100'] += car_data.get('importe_total',0.0)
                    planes_data[key]['ac_os'] += car_data.get('ac_os',0.0)

                opf_data = parse_json(ask_claude(
                    pdf_to_content(read_file(opf_file), 'OPF UNIÓN PERSONAL') + [{"type":"text","text":"Buscar línea UNION PERSONAL (SIFAR) con descripción que empiece con Efvo. La fecha_opf es la Fecha del encabezado. El nro_comprobante_opf es el Comprobante del encabezado. Extraé: {\"efvo_up\":0.0,\"fecha_opf\":\"DD/MM/YYYY\",\"nro_comprobante_opf\":\"\"}"}],
                    SYSTEM_JSON))
                pre_data = parse_json(ask_claude(
                    pdf_to_content(read_file(pre_file), 'PRE UNIÓN PERSONAL') + [{"type":"text","text":"Extraé: {\"fecha_presentacion\":\"DD/MM/YYYY\",\"nro_comprobante\":0,\"deb_cred_os\":0.0,\"bonificaciones\":0.0,\"fdo_prest_colfarma\":0.0,\"retencion_colegio_art12\":0.0,\"total_liquidacion\":0.0}. deb_cred_os = DEB/CRED DE OBRA SOCIAL (negativo si es débito, 0 si no aparece). bonificaciones=BONIFICACIONES, fdo_prest_colfarma=FDO PREST COLFARMA, total_liquidacion=Total liquidación."}],
                    SYSTEM_JSON))
                pago_data = parse_json(ask_claude(
                    pdf_to_content(read_file(pago_file), 'PAGO FINAL UNIÓN PERSONAL') + [{"type":"text","text":f"La fecha_pago es la Fecha del encabezado. El nro_comprobante_pago es el Comprobante del encabezado. Buscar línea UNION PERSONAL con liquidación nro {pre_data.get('nro_comprobante','')}. Extraé: {{\"fecha_pago\":\"DD/MM/YYYY\",\"nro_comprobante_pago\":\"\"}}"}],
                    SYSTEM_JSON))

                buf = build_unionpersonal_excel(
                    {'planes':planes_data,'opf':opf_data,'pre':pre_data,'pago':pago_data,'fecha_cierre':fecha_cierre},
                    mes_num, anio[-2:])
                reportes.append((f"{anio[-2:]}.{mes_num} - Reporte Union Personal.xlsx", buf.getvalue()))
            except Exception as e:
                errores.append(f"Error en {rel}: {str(e)}")

        if not reportes:
            raise HTTPException(status_code=400, detail=f"No se generaron reportes. Errores: {errores}")
        with zipfile.ZipFile(output_zip, 'w') as zout:
            for filename, data in reportes:
                zout.writestr(filename, data)
            if errores:
                zout.writestr("errores.txt", "\n".join(errores))

    output_zip.seek(0)
    return StreamingResponse(output_zip, media_type='application/zip',
                             headers={'Content-Disposition': 'attachment; filename="Reportes_UP.zip"'})
