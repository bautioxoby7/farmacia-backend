from fastapi import APIRouter, HTTPException, Request
from fastapi.responses import JSONResponse
import json
import base64
from main import client, parse_json

router = APIRouter()


# ══════════════════════════════════════════════════════════════════════════════
# DÉBITOS PAMI — El scraping lo hace el frontend (browser del usuario)
# El backend solo recibe los datos y genera el análisis con IA
# ══════════════════════════════════════════════════════════════════════════════

SYSTEM_DEBITOS = "CRÍTICO: Respondé ÚNICAMENTE con el objeto JSON solicitado. Sin texto, sin explicaciones, sin markdown. Empezá con { y terminá con }."


async def analizar_recetas_con_ia(ajustes: list[dict]) -> dict:
    """
    Analiza cada receta con Claude Vision.
    Cada receta tiene dos imágenes (base64) en arch["img_001"] y arch["img_002"].
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
        # Construir contenido con imágenes si están disponibles
        content_parts = []

        for key in ["img_001", "img_002"]:
            img_b64 = receta.get(key)
            if img_b64:
                # Quitar prefijo data:image/png;base64, si existe
                if "," in img_b64:
                    img_b64 = img_b64.split(",", 1)[1]
                content_parts.append({
                    "type": "image",
                    "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}
                })

        nombre = receta["nombre"]
        error = receta["error_pami"]
        prompt = (
            f"Analiza esta receta PAMI debitada. Numero de receta: {nombre}. "
            f"Error PAMI indicado: {error}. "
            "Extrae en JSON: numero_receta, afiliado_nombre, afiliado_numero, "
            "medico_matricula, "
            "medicamentos (lista con nombre/cantidad/troquel_prescripto), "
            "troqueles_pegados (lista de codigos visibles en la receta), "
            "error_detectado (por que difieren troquel pegado y prescripto), "
            "accion_correctiva, gravedad (alta/media/baja)"
        )

        content_parts.append({"type": "text", "text": prompt})

        try:
            msg = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=800,
                system=SYSTEM_DEBITOS,
                messages=[{"role": "user", "content": content_parts}]
            )
            resultado = parse_json(msg.content[0].text)
            resultado["error_pami"] = receta["error_pami"]
            recetas_analizadas.append(resultado)
        except Exception as e:
            recetas_analizadas.append({
                "numero_receta": receta["nombre"],
                "error_pami": receta["error_pami"],
                "error_detectado": f"Error al analizar: {str(e)}",
                "gravedad": "media"
            })

    # Resumen general
    try:
        errores = [r.get("error_detectado", "") for r in recetas_analizadas]
        resumen_prompt = (
            f"Analiza estos {len(recetas_analizadas)} errores de débito PAMI de una farmacia: "
            + "; ".join(errores[:10])
            + ". Genera JSON: {conclusion, error_principal, recomendaciones: [3 recomendaciones concretas]}"
        )
        msg_res = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            system=SYSTEM_DEBITOS,
            messages=[{"role": "user", "content": resumen_prompt}]
        )
        resumen = parse_json(msg_res.content[0].text)
    except Exception:
        resumen = {"conclusion": f"Se analizaron {len(recetas_analizadas)} recetas debitadas", "recomendaciones": []}

    return {"recetas_analizadas": recetas_analizadas, "resumen": resumen}



# ══════════════════════════════════════════════════════════════════════════════
# DÉBITOS PAMI
# ══════════════════════════════════════════════════════════════════════════════

SYSTEM_DEBITOS = "CRITICO: Responde UNICAMENTE con el objeto JSON solicitado. Sin texto, sin explicaciones, sin markdown. Empieza con { y termina con }."


async def analizar_recetas_con_ia(ajustes: list[dict]) -> dict:
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
        for key in ["img_001", "img_002"]:
            img_b64 = receta.get(key)
            if img_b64:
                if "," in img_b64:
                    img_b64 = img_b64.split(",", 1)[1]
                content_parts.append({
                    "type": "image",
                    "source": {"type": "base64", "media_type": "image/jpeg", "data": img_b64}
                })

        nombre = receta["nombre"]
        error = receta["error_pami"]
        prompt = (
            "Analiza esta receta PAMI debitada. Numero de receta: " + nombre + ". "
            "Error PAMI indicado: " + error + ". "
            "Extrae en JSON: numero_receta, afiliado_nombre, afiliado_numero, "
            "medico_matricula, "
            "medicamentos (lista con nombre/cantidad/troquel_prescripto), "
            "troqueles_pegados (lista de codigos visibles en la receta), "
            "error_detectado (por que difieren troquel pegado y prescripto), "
            "accion_correctiva, gravedad (alta/media/baja)"
        )
        content_parts.append({"type": "text", "text": prompt})

        try:
            msg = client.messages.create(
                model="claude-sonnet-4-20250514",
                max_tokens=800,
                system=SYSTEM_DEBITOS,
                messages=[{"role": "user", "content": content_parts}]
            )
            resultado = parse_json(msg.content[0].text)
            resultado["error_pami"] = receta["error_pami"]
            recetas_analizadas.append(resultado)
        except Exception as e:
            recetas_analizadas.append({
                "numero_receta": receta["nombre"],
                "error_pami": receta["error_pami"],
                "error_detectado": str(e),
                "gravedad": "media"
            })

    try:
        errores = [r.get("error_detectado", "") for r in recetas_analizadas]
        resumen_prompt = (
            "Analiza estos " + str(len(recetas_analizadas)) + " errores de debito PAMI: "
            + "; ".join(errores[:10])
            + ". Genera JSON: {conclusion, error_principal, recomendaciones: [3 recomendaciones]}"
        )
        msg_res = client.messages.create(
            model="claude-sonnet-4-20250514",
            max_tokens=500,
            system=SYSTEM_DEBITOS,
            messages=[{"role": "user", "content": resumen_prompt}]
        )
        resumen = parse_json(msg_res.content[0].text)
    except Exception:
        resumen = {"conclusion": str(len(recetas_analizadas)) + " recetas debitadas", "recomendaciones": []}

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
        return JSONResponse({"periodo": periodo, "ajustes": [], "total_recetas": 0,
                             "total_monto": 0, "resumen_ia": None,
                             "mensaje": "No se encontraron debitos"})

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
            {"nota": k, "count": v, "porcentaje": round(v/total_recetas*100, 1)}
            for k, v in sorted(errores.items(), key=lambda x: x[1], reverse=True)
        ],
        "recetas_analizadas": analisis.get("recetas_analizadas", []),
        "resumen_ia": analisis.get("resumen", {})
    })


# Cache en memoria para el último análisis
_ultimo_analisis_cache = {}

@router.post("/debitos/guardar")
async def guardar_analisis(request: Request):
    """La extensión guarda el análisis acá. La app lo lee después."""
    try:
        body = await request.json()
        farmacia_id = body.get("farmacia_id", "default")
        _ultimo_analisis_cache[farmacia_id] = body
        return JSONResponse({"ok": True})
    except Exception as e:
        raise HTTPException(status_code=400, detail=str(e))

@router.get("/debitos/ultimo")
async def obtener_ultimo_analisis(farmacia_id: str = "default"):
    """La app lee el último análisis guardado por la extensión."""
    data = _ultimo_analisis_cache.get(farmacia_id)
    if not data:
        return JSONResponse({"disponible": False})
    return JSONResponse({"disponible": True, "data": data})

