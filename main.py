from fastapi import FastAPI, HTTPException
from fastapi.responses import JSONResponse
from typing import Optional
import uvicorn
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import re
import os
import urllib.parse

description = """
API de búsqueda para trámites de la Secretaria de Igualdad e Inclusión del gobierno de Nuevo León. 🦁´

Tiene un único endpoint, **/buscar**

## Buscar

Puedes buscar usuarios que hayan sigo acreedores o estén en proceso de un trámite de la secretaria.
Tiene 3 opciones de búsqueda:

* CURP
* Nombres
* Apellidos

Para seleccionar la opción de búsqueda, basta con mandar como parámetro en la url el método, es decir:

https://url/buscar?metodo=curp

Si no se selecciona ninguna opción, le hará query de todos los usuarios en la DB. Para no sobrecargar la respuesta,
los usuarios que regresa están paginados. Los parámetros usados en la paginación son:

* page
* page_size

También pueden ser agregados como parámetro en la url.
"""

app = FastAPI(
    title="API de búsqueda SII",
    description=description,
    version="0.0.1",
    terms_of_service="http://example.com/terms/",
    contact={
        "name": "Zaid De Anda, desarrollador",
        "email": "zaidy.deanda@gmail.com",
    })

# Configuración de logging
logger = logging.getLogger("my_logger")
logger.setLevel(logging.DEBUG)

# Configurar el manejador de registro para escribir en un archivo de registro
handler = RotatingFileHandler("app.log", maxBytes=1024 * 1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Cargar el archivo JSON
basePath = os.path.dirname(os.path.abspath(__file__))
file = basePath+"/data_good.json"
print(f"file location: {file}")
data = pd.read_json(file, encoding="utf-8")
data["apellidos"] = data["ap_materno"] + " " + data["ap_paterno"]
data = data.fillna(value=0)

# Expresión regular para validar CURP
CURP_REGEX = r'^[A-Z]{4}\d{6}[HM][A-Z]{5}[0-9A-Z]\d$'

def validate_curp(curp):
    if re.match(CURP_REGEX, curp):
        return True
    return False

# Ruta para buscar un registro por CURP o listar todos los registros
@app.get("/buscar/")
async def buscar_registros(
    metodo: Optional[str] = None,
    valor: Optional[str] = None,
    page: int = 1,
    page_size: int = 10,
):
    start_idx = (page - 1) * page_size
    end_idx = start_idx + page_size

    if metodo is None:
        logger.info("Se recibio request de toda la tabla")
        filtered_data = data
        total_records = len(data)
    else:
        if valor is None:
            logger.error(f"Se recibio un request con método {metodo} pero sin valor de busqueda")
            raise HTTPException(status_code=400, detail="El parámetro 'valor' es obligatorio cuando se proporciona 'método'")
        if metodo not in ("apellidos", "nombres", "curp"):
            logger.error(f"Se recibio un request con método inválido {metodo}")
            raise HTTPException(status_code=400, detail="Método de búsqueda no válido")
        logger.info(f"Se recibio un request con metodo {metodo}")
        valor = urllib.parse.unquote(valor)
        if metodo == "apellidos":
            filtered_data = data.loc[data["apellidos"] == valor]
        elif metodo == "nombres":
            filtered_data = data.loc[data["nombres"] == valor]
        elif metodo == "curp":
            if not validate_curp(valor):
                logger.error(f"Se recibio un request con metodo {metodo} pero el curp no tenia un formato valido")
                logger.error(f"Curp enviado: {valor}")
                raise HTTPException(status_code=400, detail="CURP no válido")
            filtered_data = data.loc[data["CURP"] == valor]
        total_records = len(filtered_data)
        if total_records < 1:
            logger.error(f"Se recibio un request con metodo {metodo} pero no fue encontrado")
            raise HTTPException(status_code=404, detail=f"{metodo} no encontrado")
    
    if start_idx >= total_records:
        return []

    if end_idx >= total_records:
        end_idx = total_records

    paginated_data = filtered_data.iloc[start_idx:end_idx]
    final_data = paginated_data.to_dict(orient="records")[0]
    response = {
        "status" : "success",
        "data" : final_data,
        "mensaje" : "Query realizado con éxito",
        "status_code" : 200
    }
    return JSONResponse(content=response)

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)