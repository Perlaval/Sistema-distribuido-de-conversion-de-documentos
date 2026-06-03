from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException, WebSocket
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from minio import Minio
from minio.error import S3Error
import redis
import uuid
import io
import os
import io
import zipfile
import asyncio


app = FastAPI()

## CORS para que el navegador no bloquee la respuesa
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_methods=["*"],
    allow_headers=["*"],
)

VALKEY_HOST = os.getenv("VALKEY_HOST", "localhost")
VALKEY_PORT = int(os.getenv("VALKEY_PORT", 6379))
VALKEY_PASSWORD = os.getenv("VALKEY_PASSWORD", "")

#MINIO_ENDPOINT = os.getenv("MINIO_ENDPOINT", "localhost:9000")
MINIO_HOST = os.getenv("MINIO_HOST", "myminio") #"minio:9000"
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

minio_client = Minio(MINIO_HOST, 
    access_key=MINIO_ACCESS_KEY, 
    secret_key=MINIO_SECRET_KEY, 
    secure=False)


@app.on_event("startup")
async def startup():
    # Crear el bucket si no existe al arrancar el backend
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
        print(f"Bucket '{BUCKET}' creado")

@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...)):

    #--------para recibir de a 1 pdf-----------------------------------------------------------------------------------------
    
    # Validar que sea un PDF (se podria validar en el front pero si vamos a permitir un.zip creo que lo necesitamos)
    if file.filename.endswith(".pdf"):
        #raise HTTPException(status_code=400, detail="Solo se aceptan archivos PDF")
        
        # Genero el identificador único global (UUID)
        job_id = str(uuid.uuid4())
        file_path = f"pdfs/{job_id}.pdf"
        #file_extension = file.filename.split(".")[-1]
        #object_name = f"{task_id}.{file_extension}"

        # Subo el archivo a MinIO
        # El backend guarda el pdf fisicamente
        #s3_client.upload_fileobj(file.file, "input-pdfs", object_name)
        contenido = await file.read()
        minio_client.put_object(BUCKET, file_path, io.BytesIO(contenido), length = len(contenido), content_type = "application/pdf")

        # Publico el mensaje en el stream de Valkey (le paso el id del pdf que genere con uuid)
        redis_client.xadd(STREAM, {"job_id": job_id, "file_path": file_path})
        #redis_client.lpush("task_queue", task_id)

        # le coloco el estado inicial 
        redis_client.set(f"estado:{job_id}", "Pendiente")

        # Msj inmediato al cliente con el UUID 
        return {"job_id": job_id, "message": "Archivo recibido y en cola de procesamiento"}
    
    # ----------para recibir zip --------------------------------------------------------------

    elif file.filename.endswith(".zip"):

        contenido_zip = await file.read()

        zip_buffer = io.BytesIO(contenido_zip)

        jobs = []

        with zipfile.ZipFile(zip_buffer, "r") as zip_ref:
            
            #usamos infolist pq el zip puede tener adentro otras carpetas entonces si lanzamos la excepcion porque cualquier archivo no es .pdf no me procesa lo que tengo adentro del zip 
            for info in zip_ref.infolist():
                
                if info.is_dir():
                    continue
                
                if not info.filename.lower().endswith(".pdf"):
                    continue

                #if nombre_archivo.endswith(".pdf"):

                pdf_data = zip_ref.read(info.filename)

                job_id = str(uuid.uuid4())

                file_path = f"pdfs/{job_id}.pdf"

                minio_client.put_object(
                    BUCKET,
                    file_path,
                    io.BytesIO(pdf_data),
                    length=len(pdf_data),
                    content_type="application/pdf"
                )

                redis_client.xadd(STREAM, {
                    "job_id": job_id,
                    "file_path": file_path
                })

                jobs.append(job_id)

                redis_client.set(f"estado:{job_id}", "Pendiente")

            return {
                "mensaje": "ZIP procesado",
                "cantidad_pdfs": len(jobs),
                "jobs": jobs
            }
                #else:
                    #raise HTTPException(status_code=400, detail="Solo se aceptan archivos PDF")
     # ---------------- ERROR ----------------

    else:
        raise HTTPException(
            status_code=400,
            detail="Solo se aceptan archivos PDF o ZIP"
        )
    

@app.get("/estado/{job_id}")
async def get_status(job_id: str):
    #Primero chequeamos si el archivo ya esta en MinIO
    try:
        minio_client.stat_object(BUCKET, f"txt/{job_id}.txt")
        #si no lanza excepción, el archivo existe y su estado es completado
        redis_client.set(f"estado:{job_id}", "Completado")
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


## para descargar el archivo ya convertido
@app.get("/download/{job_id}")
async def download_file(job_id: str):

    try:
        obj = minio_client.get_object(
            BUCKET,
            f"txt/{job_id}.txt"
        )

        return StreamingResponse(
            obj,
            media_type="text/plain",
            headers={
                "Content-Disposition":
                f'attachment; filename="{job_id}.txt"'
            }
        )

    except Exception:
        raise HTTPException(
            status_code=404,
            detail="Archivo no encontrado"
        )

## web socket para comunicacion con el cliente
@app.websocket("/ws/{job_id}")
async def websocket_status(websocket: WebSocket, job_id: str):
    await websocket.accept()
    while True:
        # Reutilizás la misma lógica de /estado
        try:
            minio_client.stat_object(BUCKET, f"txt/{job_id}.txt")
            redis_client.set(f"estado:{job_id}", "Completado")
            await websocket.send_json({"estado": "Completado"})
            break  # estado final, cerramos
        except S3Error:
            pass

        status = redis_client.get(f"estado:{job_id}")
        if status:
            await websocket.send_json({"estado": status})
        else:
            await websocket.send_json({"estado": "Tarea no encontrada"})

        await asyncio.sleep(2)

    await websocket.close()



