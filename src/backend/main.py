from fastapi import FastAPI, UploadFile, File, BackgroundTasks
import uuid
import boto3
import redis
import os

app = FastAPI()

# Configuración desde variables de entorno
VALKEY_HOST = os.getenv("VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.getenv("VALKEY_PORT", 6379))
VALKEY_PASSWORD = os.getenv("VALKEY_PASSWORD", "")

#MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
REDIS_HOST = os.getenv("REDIS_HOST", "localhost")

BUCKET = "documentos"
STREAM = "trabajos"

# Clientes de infraestructura
redis_client = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PASSWORD, decode_responses=True)
minio_client = Minio(MINIO_HOST, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

#s3_client = boto3.client('s3', endpoint_url=f"http://{MINIO_ENDPOINT}",
                        # aws_access_key_id=MINIO_ACCESS_KEY,
                        # aws_secret_access_key=MINIO_SECRET_KEY)

#redis_client = redis.Redis(host=REDIS_HOST, port=6379, db=0)

@app.on_event("startup")
async def startup():
    # Crear el bucket si no existe al arrancar el backend
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
        print(f"Bucket '{BUCKET}' creado")

@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):
    
    # Validar que sea un PDF (se podria validar en el front pero si vamos a permitir un.zip creo que lo necesitamos)
    if not file.filename.endswith(".pdf"):
        raise HTTPException(status_code=400, detail="Solo se aceptan archivos PDF")
    
    
    # Genero el identificador único global (UUID)
    job_id = str(uuid.uuid4())
    file_path = f"pdfs/{job_id}.pdf"
    #file_extension = file.filename.split(".")[-1]
    #object_name = f"{task_id}.{file_extension}"

    # Subo el archivo a MinIO
    # El backend guarda el pdf fisicamente
    #s3_client.upload_fileobj(file.file, "input-pdfs", object_name)
    contenido = await file.read()
    minio_client.put_object(BUCKET, filepath, io.BytesIO(contenido), length = len(contenido, content_type = "application/pdf"))

    # Publico el mensaje en el stream de Valkey (le paso el id del pdf que genere con uuid)
    redis_client.xadd(STREAM, {"job_id": job_id, "file_path": file_path})
    #redis_client.lpush("task_queue", task_id)

    # le coloco el estado inicial 
    redis_client.set(f"estado:{job_id}", "Pendiente")

    # Msj inmediato al cliente con el UUID 
    return {"job_id": job_id, "message": "Archivo recibido y en cola de procesamiento"}


@app.get("/estado/{job_id}")
async def get_status(job_id: str):
    #Primero chequeamos si el archivo ya esta en MinIO
    try:
        minio_client.stat_object(BUCKET, f"txt/{job_id}.txt")
        #si no lanza excepción, el archivo existe y su estado es completado
        r.set(f"estado:{job_id}", "Completado")
        return {"job_id": job_id, "estado":"Completado"}
    except S3Error:
        pass

    #Si no esta en MinIO, consultamos Valkey
    # Endpoint para que el cliente haga Polling mediante AJAX [11]
    status = redis_client.get(f"estado:{job_id}")
    if not status:
        return {"error": "Tarea no encontrada"}
    return {"job_id": job_id, "estado": status} #status.decode("utf-8")

@app.get("/resultado/{job_id}")
async def get_resultado(job_id: str):
    # Endpoint para descargar el TXT resultante
    try:
        response = minio_client.get_object(BUCKET, f"txt/{job_id}.txt")
        return StreamingResponse(
            response,
            media_type="text/plain",
            headers={"Content-Disposition": f"attachment; filename={job_id}.txt"}
        )
    except S3Error:
        raise HTTPException(status_code=404, detail="Resultado no disponible todavía")