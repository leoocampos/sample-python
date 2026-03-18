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

        # 🚀 CORREÇÃO 2: Caso a pasta esteja vazia
        if not arquivos:
            logging.info("A pasta do Google Drive está vazia.")
            return {
                "status": "no_files", # Mudamos de 'success' para 'no_files'
                "message": "A pasta de origem não contém arquivos para processar.",
                "processed_files": 0
            }, 200

        arquivos_copiados = 0
        for arq in arquivos:
            sucesso = mover_para_bucket(drive_service, bucket, arq["id"], arq["name"])
            if sucesso:
                arquivos_copiados += 1

        # Retorno final baseado na execução
        return {
            "status": "success",
            "processed_files": arquivos_copiados
        }, 200

    except Exception as e:
        logging.error(f"Erro geral: {e}")
        return {"status": "error", "message": str(e)}, 500

@app.post("/file_transfer")
def file_transfer():
    resultado, status_code = processar_arquivos()
    return jsonify(resultado), status_code

if __name__ == "__main__":
    app.run(host="0.0.0.0", port=8080)