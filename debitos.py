from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
import json
import base64
from main import client, parse_json

router = APIRouter()

SYSTEM_DEBITOS = "CRITICO: Responde UNICAMENTE con el objeto JSON solicitado. Sin texto, sin explicaciones, sin markdown. Empieza con { y termina con }."


def seleccionar_imagen_frente(img_001: str, img_002: str) -> str:
    """
    Devuelve la imagen del frente de la receta.
    El frente tiene mas contenido (medicamentos, precios, troqueles) y pesa mas.
    """
    if img_001 and img_002:
        tam_001 = len(img_001.split(",", 1)[1] if "," in img_001 else img_001)
        tam_002 = len(img_002.split(",", 1)[1] if "," in img_002 else img_002)
        return img_001 if tam_001 > tam_002 else img_002
    return img_002 or img_001


def detectar_intercambio_cruzado(recetas: list) -> list:
    """
    Detecta recetas del mismo afiliado donde los troqueles incorrectos
    de una coinciden con los medicamentos de la otra.
    """
    por_afiliado = {}
    for r in recetas:
        num = r.get("afiliado_numero") or r.get("afiliado_nombre", "desconocido")
        if num not in por_afiliado:
            por_afiliado[num] = []
        por_afiliado[num].append(r)

    for afiliado, grupo in por_afiliado.items():
        if len(grupo) < 2:
            continue
        for i in range(len(grupo)):
            for j in range(i + 1, len(grupo)):
                r1, r2 = grupo[i], grupo[j]
                meds_r1 = [m.get("nombre", "").upper()[:6] for m in r1.get("medicamentos", []) if m.get("nombre")]
                meds_r2 = [m.get("nombre", "").upper()[:6] for m in r2.get("medicamentos", []) if m.get("nombre")]
                troqs_r1 = [str(t.get("descripcion", "") if isinstance(t, dict) else t).upper() for t in r1.get("troqueles_pegados", [])]
                troqs_r2 = [str(t.get("descripcion", "") if isinstance(t, dict) else t).upper() for t in r2.get("troqueles_pegados", [])]

                cruce = any(m in t for m in meds_r1 for t in troqs_r2 if len(m) > 3) or \
                        any(m in t for m in meds_r2 for t in troqs_r1 if len(m) > 3)

                if cruce:
                    r1["intercambio_cruzado"] = True
                    r1["intercambio_con"] = r2.get("numero_receta", "")
                    r2["intercambio_cruzado"] = True
                    r2["intercambio_con"] = r1.get("numero_receta", "")

    return recetas


async def analizar_recetas_con_ia(ajustes: list) -> dict:
    """
    Analiza cada receta con Claude Vision usando solo el frente.
    """
    todas_las_recetas = []
    for aj in ajustes:
        for arch in aj.get("archivos", []):
            todas_las_recetas.append({
                "ajuste": aj["id"],
                "nombre": arch["nombre"],
                "error_pami": arch.get("nota", ""),
                "img_001": arch.get("img_001"),
                "img_002": arch.get("img_002"),
            })

    if not todas_las_recetas:
        return {"recetas_analizadas": [], "resumen": {}}

    recetas_analizadas = []

    for receta in todas_las_recetas:
        content_parts = []

        img_frente = seleccionar_imagen_frente(receta.get("img_001"), receta.get("img_002"))

        if img_frente:
            img_b64 = img_frente.split(",", 1)[1] if "," in img_frente else img_frente
            content_parts.append({
                "type": "image",
                "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}
            })

        nombre = receta["nombre"]
        error = receta["error_pami"]

        prompt = (
            "Analiza esta receta PAMI debitada siguiendo estos pasos en orden.\n\n"
            "PASO 1 - ORIENTACION: Si la imagen esta girada, rotala mentalmente para leerla.\n\n"
            "PASO 2 - AFILIADO: El nombre del afiliado figura en la linea que dice Afiliado: NUMERO - APELLIDO NOMBRE. "
            "NO confundir con el nombre de la farmacia (MERLO o Farmacia Merlo) que tambien aparece en la receta. "
            "El afiliado es la PERSONA que recibe el medicamento, no la farmacia.\n\n"
            "PASO 3 - MEDICAMENTOS PRESCRIPTOS: Identificar cada medicamento con nombre completo, "
            "cantidad de unidades, precio unitario, precio total y porcentaje de cobertura PAMI "
            "(el % figura al lado del precio, ej: 100%, 60%, 50%, 40%).\n\n"
            "PASO 4 - TROQUELES PEGADOS: Leer cada troquel pegado en la receta. "
            "Identificar el medicamento leyendo el TEXTO del troquel "
            "(nombre, mg, cantidad, forma farmaceutica). No adivinar por el codigo de barras.\n\n"
            "PASO 5 - COMPARACION por cada medicamento prescripto:\n"
            "  CORRECTO: troquel coincide exactamente en nombre, mg, cantidad y presentacion\n"
            "  DIFIERE: es de otro medicamento o presentacion diferente\n"
            "  FALTA: no hay troquel para ese medicamento\n"
            "  DUPLICADO: mismo troquel mas de una vez (uno correcto, los demas incorrectos)\n\n"
            "PASO 6 - CALCULO para cada medicamento INCORRECTO:\n"
            "  Los precios en la receta usan punto como separador de miles y coma como decimal.\n"
            "  Ejemplo: 25.767,66 significa VEINTICINCO MIL SETECIENTOS SESENTA Y SIETE con 66 centavos.\n"
            "  NO confundir: 25.767,66 NO es 25.7 ni 257.67, ES 25767.66\n"
            "  Formula: monto = precio_total x (porcentaje_cobertura - 30) / 100\n"
            "  Ej precio 25767.66 con 100%: 25767.66 x 70 / 100 = 18037.36\n"
            "  Ej precio 13505.15 con 60%: 13505.15 x 30 / 100 = 4051.55\n"
            "  En el JSON, monto_debitado debe ser un numero sin puntos ni comas, ej: 18037.36\n"
            "  Sumar todos los incorrectos para el total.\n\n"
            "PASO 7 - IMPORTANTE - IDENTIFICAR EL FRENTE DE LA RECETA:\n"
            "  El FRENTE tiene: nombre del afiliado, medicamentos, precios, troqueles pegados.\n"
            "  El DORSO tiene: solo sello de farmacia y firma.\n"
            "  Analizar SOLO el frente. Si la imagen es el dorso, indicar confianza_baja: true.\n\n"
            "PASO 8 - CONFIANZA: si hay dudas por imagen borrosa, troqueles ilegibles o "
            "si solo se ve el dorso de la receta, marcar confianza_baja: true y explicar.\n\n"
            "Numero de receta: " + nombre + ". Error PAMI indicado por COFA: " + error + ".\n"
            "IMPORTANTE: el error PAMI indica r1=primer medicamento mal, r2=segundo medicamento mal.\n\n"
            "Responder SOLO con JSON:\n"
            "numero_receta, afiliado_nombre, afiliado_numero, medico_matricula,\n"
            "medicamentos [{nombre, cantidad, precio_unitario, precio_total, cobertura_pct, troquel_estado, troquel_descripcion}],\n"
            "troqueles_pegados [{codigo, descripcion}],\n"
            "error_detectado, monto_debitado (numero decimal sin separadores de miles),\n"
            "detalle_calculo, confianza_baja, motivo_duda, accion_correctiva, gravedad"
        )

        content_parts.append({"type": "text", "text": prompt})

        try:
            msg = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=1500,
                system=SYSTEM_DEBITOS,
                messages=[{"role": "user", "content": content_parts}]
            )
            resultado = parse_json(msg.content[0].text)
            resultado["error_pami"] = receta["error_pami"]
            resultado["ajuste"] = receta["ajuste"]
            recetas_analizadas.append(resultado)
        except Exception as e:
            recetas_analizadas.append({
                "numero_receta": receta["nombre"],
                "error_pami": receta["error_pami"],
                "ajuste": receta["ajuste"],
                "error_detectado": f"Error al analizar: {str(e)}",
                "monto_debitado": 0,
                "confianza_baja": True,
                "motivo_duda": str(e),
                "gravedad": "media"
            })

    # Chequeo cruzado de pacientes
    recetas_analizadas = detectar_intercambio_cruzado(recetas_analizadas)

    # Resumen general
    try:
        errores_txt = [r.get("error_detectado", "") for r in recetas_analizadas]
        intercambios = [r for r in recetas_analizadas if r.get("intercambio_cruzado")]
        resumen_prompt = (
            "Analiza estos " + str(len(recetas_analizadas)) + " errores de debito PAMI: "
            + "; ".join(errores_txt[:10])
            + ". Genera JSON: {conclusion, error_principal, recomendaciones: [3 acciones concretas]}"
        )
        msg_res = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            system=SYSTEM_DEBITOS,
            messages=[{"role": "user", "content": resumen_prompt}]
        )
        resumen = parse_json(msg_res.content[0].text)
        if intercambios:
            resumen["alerta_intercambio"] = (
                f"Se detectaron {len(intercambios)} recetas del mismo afiliado con posible "
                "intercambio de troqueles. Revisar para posible reclamo a PAMI."
            )
    except Exception:
        resumen = {"conclusion": f"{len(recetas_analizadas)} recetas analizadas", "recomendaciones": []}

    return {"recetas_analizadas": recetas_analizadas, "resumen": resumen}


@router.post("/debitos/analizar")
async def analizar_debitos(request: Request):
    try:
        body = await request.json()
    except Exception:
        raise HTTPException(status_code=400, detail="Body JSON invalido")

    periodo = body.get("periodo", "")
    ajustes = body.get("ajustes", [])

    if not ajustes:
        return JSONResponse({
            "periodo": periodo, "ajustes": [], "total_recetas": 0,
            "total_monto": 0, "resumen_ia": None,
            "mensaje": "No se encontraron debitos"
        })

    analisis = await analizar_recetas_con_ia(ajustes)

    errores: dict = {}
    total_recetas = 0
    for aj in ajustes:
        for arch in aj.get("archivos", []):
            nota = arch.get("nota", "Desconocido")
            errores[nota] = errores.get(nota, 0) + 1
            total_recetas += 1

    return JSONResponse({
        "periodo": periodo,
        "ajustes": ajustes,
        "total_recetas": total_recetas,
        "total_monto": round(sum(aj.get("monto", 0) for aj in ajustes), 2),
        "distribucion_errores": [
            {"nota": k, "count": v, "porcentaje": round(v / total_recetas * 100, 1)}
            for k, v in sorted(errores.items(), key=lambda x: x[1], reverse=True)
        ],
        "recetas_analizadas": analisis.get("recetas_analizadas", []),
        "resumen_ia": analisis.get("resumen", {})
    })


# Cache en memoria para el último análisis
_ultimo_analisis_cache = {}


@router.post("/debitos/guardar")
async def guardar_analisis(request: Request):
    try:
        body = await request.json()
        farmacia_id = body.get("farmacia_id", "default")
        _ultimo_analisis_cache[farmacia_id] = body
        return JSONResponse({"ok": True})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))


@router.get("/debitos/ultimo")
async def obtener_ultimo_analisis(farmacia_id: str = "default"):
    data = _ultimo_analisis_cache.get(farmacia_id)
    if not data:
        return JSONResponse({"disponible": False})
    return JSONResponse({"disponible": True, "data": data})
