from flask import Flask, jsonify
from googleapiclient.discovery import build
from google.cloud import storage
from google.auth import default
from googleapiclient.errors import HttpError
import tempfile
import os
import logging

# =====================
# CONFIGURAÇÕES
# =====================
FOLDER_ORIGEM = "12hs-FDKNkljlRuN8jslq9yaTHRUca_b4"
BUCKET_NAME = "sample-track-files"

app = Flask(__name__)
logging.basicConfig(level=logging.INFO)

def get_clients():
    SCOPES = ["https://www.googleapis.com/auth/drive"]
    creds, _ = default(scopes=SCOPES)
    drive_service = build("drive", "v3", credentials=creds)
    storage_client = storage.Client()
    bucket = storage_client.bucket(BUCKET_NAME)
    return drive_service, bucket

def listar_arquivos_pasta(drive_service, folder_id):
    query = f"'{folder_id}' in parents and trashed=false"
    results = drive_service.files().list(q=query).execute()
    return results.get("files", [])

def mover_para_bucket(drive_service, bucket, file_id, file_name):
    temp_path = os.path.join(tempfile.gettempdir(), file_name)
    try:
        # 1️⃣ Download do Drive
        request = drive_service.files().get_media(fileId=file_id)
        with open(temp_path, "wb") as f:
            f.write(request.execute())

        # 2️⃣ Upload para o Bucket (Sobrescreve se já existir)
        bucket.blob(file_name).upload_from_filename(temp_path)
        
        logging.info(f"Arquivo {file_name} copiado para o Bucket com sucesso.")
        return True
    except Exception as e:
        logging.error(f"Erro ao copiar {file_name}: {e}")
        return False
    finally:
        if os.path.exists(temp_path):
            os.remove(temp_path)

def processar_arquivos():
    try:
        drive_service, bucket = get_clients()
        arquivos = listar_arquivos_pasta(drive_service, FOLDER_ORIGEM)

        # 🚀 MUDANÇA AQUI: Se não houver arquivos, retornamos ERRO (404)
        if not arquivos or len(arquivos) == 0:
            logging.warning("Pasta vazia. Disparando erro para o Workflow.")
            return {
                "status": "no_files_error",
                "message": "A pasta de origem esta vazia. O pipeline nao pode continuar."
            }, 404
        arquivos_copiados = 0
        for arq in arquivos:
            sucesso = mover_para_bucket(drive_service, bucket, arq["id"], arq["name"])
            if sucesso:
                arquivos_copiados += 1
        # Caso tenha arquivos mas nenhum foi copiado com sucesso
        if arquivos_copiados == 0:
            return {
                "status": "transfer_failure",
                "message": "Arquivos encontrados, mas falha no download/upload."
            }, 500

        # SUCESSO REAL (Apenas aqui retorna 200)
        return {
            "status": "success",
            "processed_files": arquivos_copiados
        }, 200

    except Exception as e:
        logging.error(f"Erro critico: {e}")
        return {"status": "error", "message": str(e)}, 500

@app.post("/file_transfer")
def file_transfer():
    resultado, status_code = processar_arquivos()
    return jsonify(resultado), status_code

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)