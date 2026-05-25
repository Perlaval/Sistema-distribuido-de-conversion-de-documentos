from fastapi import FastAPI, UploadFile, File, BackgroundTasks
import uuid
import boto3
import redis
import os

app = FastAPI()

MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

# Clientes de infraestructura
s3_client = boto3.client('s3', endpoint_url=f"http://{MINIO_ENDPOINT}",
                         aws_access_key_id=MINIO_ACCESS_KEY,
                         aws_secret_access_key=MINIO_SECRET_KEY)

redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)

@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):
    # Genero el identificador único global (UUID)
    task_id = str(uuid.uuid4())
    file_extension = file.filename.split(".")[-1]
    object_name = f"{task_id}.{file_extension}"

    # Subo el archivo a MinIO
    # El backend guarda el pdf fisicamente
    s3_client.upload_fileobj(file.file, "input-pdfs", object_name)

    # Publico la tarea en Redis (le paso el id del pdf que genere con uuid)
    redis_client.lpush("task_queue", task_id)

    # le coloco el estado inicial 
    redis_client.set(f"status:{task_id}", "PENDIENTE")

    # Msj inmediato al cliente con el UUID 
    return {"task_id": task_id, "message": "Archivo recibido y en cola de procesamiento"}


@app.get("/status/{task_id}")
async def get_status(task_id: str):
    # Endpoint para que el cliente haga Polling mediante AJAX [11]
    status = redis_client.get(f"status:{task_id}")
    if not status:
        return {"error": "Tarea no encontrada"}
    return {"task_id": task_id, "status": status.decode("utf-8")}