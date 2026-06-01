from fastapi import FastAPI, UploadFile, File, HTTPException
from fastapi.responses import StreamingResponse
from minio import Minio
from minio.error import S3Error
import redis
import uuid
import io
import os

app = FastAPI()

VALKEY_HOST = os.getenv("VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.getenv("VALKEY_PORT", 6379))
VALKEY_PASSWORD = os.getenv("VALKEY_PASSWORD", "")

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "myminio:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")

BUCKET = "documentos"
STREAM = "trabajos"

redis_client = redis.Redis(
    host=VALKEY_HOST,
    port=VALKEY_PORT,
    password=VALKEY_PASSWORD,
    decode_responses=True
)

minio_client = Minio(
    MINIO_HOST,
    access_key=MINIO_ACCESS_KEY,
    secret_key=MINIO_SECRET_KEY,
    secure=False
)

@app.post("/upload")
async def upload_pdfs(files: list[UploadFile] = File(...)):
    trabajos = []

    for file in files:

        if not file.filename.lower().endswith(".pdf"):
            continue

        job_id = str(uuid.uuid4())
        file_path = f"pdfs/{job_id}.pdf"

        contenido = await file.read()

        minio_client.put_object(
            BUCKET,
            file_path,
            io.BytesIO(contenido),
            len(contenido),
            content_type="application/pdf"
        )

        redis_client.xadd(
            STREAM,
            {"job_id": job_id, "file_path": file_path}
        )

        redis_client.set(f"estado:{job_id}", "Pendiente")

        trabajos.append({
            "job_id": job_id,
            "filename": file.filename
        })

    return trabajos

@app.get("/estado/{job_id}")
async def get_status(job_id: str):

    estado = redis_client.get(f"estado:{job_id}")

    if not estado:
        raise HTTPException(
            status_code=404,
            detail="Trabajo no encontrado"
        )

    return {
        "job_id": job_id,
        "estado": estado
    }