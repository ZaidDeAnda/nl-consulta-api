from fastapi import FastAPI, HTTPException
from typing import Optional
import uvicorn
import logging
from logging.handlers import RotatingFileHandler
import pandas as pd
import re
import os
import urllib.parse

app = FastAPI()

# Configuración de logging
logger = logging.getLogger("my_logger")
logger.setLevel(logging.DEBUG)

# Configurar el manejador de registro para escribir en un archivo de registro
handler = RotatingFileHandler("app.log", maxBytes=1024 * 1024, backupCount=5)
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)

# Cargar el archivo JSON
file = os.getcwd()+"/data.json"
print(f"file location: {file}")
data = pd.read_json(file, encoding="utf-8")
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
    valor2: Optional[str] = None,
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
        try:
            logger.info(f"Se recibio un request con metodo {metodo}")
            valor = urllib.parse.unquote(valor)
            if metodo == "apellidos":
                filtered_data = data.loc[data["ap_materno"] == valor]
                filtered_data = filtered_data.loc[filtered_data["ap_paterno"] == valor2]
            elif metodo == "nombres":
                filtered_data = data.loc[data["nombres"] == valor]
            elif metodo == "curp":
                if not validate_curp(valor):
                    logger.error(f"Se recibio un request con metodo {metodo} pero el curp no tenia un formato valido")
                    logger.error(f"Curp enviado: {valor}")
                    raise HTTPException(status_code=400, detail="CURP no válido")
                filtered_data = data.loc[data["CURP"] == valor]
            total_records = len(filtered_data)
        except:
            logger.error(f"Se recibio un request con metodo {metodo} pero no fue encontrado")
            raise HTTPException(status_code=404, detail=f"{metodo} no encontrado")
    
    if start_idx >= total_records:
        return []

    if end_idx >= total_records:
        end_idx = total_records

    paginated_data = filtered_data.iloc[start_idx:end_idx]
    final_data = paginated_data.to_dict(orient="records")
    return final_data

if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)