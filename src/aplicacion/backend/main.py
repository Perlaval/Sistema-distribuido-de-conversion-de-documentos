from fastapi import FastAPI, UploadFile, File, BackgroundTasks, HTTPException, WebSocket, Form
from fastapi.responses import StreamingResponse
from fastapi.middleware.cors import CORSMiddleware
from minio import Minio
from minio.error import S3Error
import redis
#from redis.cluster import RedisCluster
import uuid
import io
import os
import io
import zipfile
import asyncio
import json
import urllib3


urllib3.disable_warnings() #para que no muestre warnings de certificado inseguro
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

"""redis_client = redis.Redis(
    host=VALKEY_HOST,
    port=VALKEY_PORT,
    password=VALKEY_PASSWORD,
    decode_responses=True,
    socket_timeout=0.5,
    socket_connect_timeout=0.5

)"""

redis_client = redis.Redis(
    host=VALKEY_HOST, 
    port=VALKEY_PORT,
    password=VALKEY_PASSWORD,
    decode_responses=True,
    socket_timeout=0.5,
    socket_connect_timeout=0.5)

minio_client = Minio(MINIO_HOST, 
    access_key=MINIO_ACCESS_KEY, 
    secret_key=MINIO_SECRET_KEY, 
    secure=True,
    http_client=urllib3.PoolManager(cert_reqs='CERT_NONE'))


@app.on_event("startup")
async def startup():
    # Crear el bucket si no existe al arrancar el backend
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
        print(f"Bucket '{BUCKET}' creado")

def procesar_zip_en_segundo_plano(ruta_zip_minio, batch_id, jobs):

    try:
        obj = minio_client.get_object(
            BUCKET,
            ruta_zip_minio
        )

        contenido_zip = obj.read()
        zip_buffer = io.BytesIO(contenido_zip)

        # Armamos el batch_info que se va a almacenar en minio
        with zipfile.ZipFile(zip_buffer, "r") as zip_ref:
            
            # El estado nos va a permitir saber si se completo la conversion de todos los pdfs del zip en caso de que se caiga el back
            # Si el job no se proceso cuando busque en esa ruta en minio voy detectar un error
            batch_info = {
                "batch_id": batch_id,
                #"estado": "Pendiente",
                "jobs": jobs
            }

            batch_json = json.dumps(batch_info).encode("utf-8")

            # Lo guardamos en minio porque si se cae redis y ya los workers habia procesado todos los pdfs del zip, el cliente deberia poder descargar su zip convertido
            minio_client.put_object(
                BUCKET,
                f"batches/{batch_id}.json",
                io.BytesIO(batch_json),
                length=len(batch_json),
                content_type="application/json"
            )

            #2. Subimos los pdfs a mino y enviamos los jobs a Valkey
            for job in jobs:
                
                pdf_data = zip_ref.read(job["path"])

                job_id = job["job_id"]

                file_path = f"pdfs/{job_id}.pdf"

                minio_client.put_object(
                    BUCKET,
                    file_path,
                    io.BytesIO(pdf_data),
                    length=len(pdf_data),
                    content_type="application/pdf"
                )

                redis_client.xadd(STREAM, {
                    #"batch_id": batch_id,
                    "job_id": job_id,
                    "file_path": file_path
                }) 

                redis_client.set(
                    f"estado:{job_id}",
                    "Pendiente"
                )
        #zip_buffer.seek(0)
        # Eliminamos path de la estructura porque no lo vamos a necesitar mas
        for job in jobs:
            job.pop("path", None)

        print(f"[BACKGROUND] Batch procesado y enviado a Valkey con éxito: {batch_id}")

    except S3Error as e:
        # Las tareas en segundo plano no pueden responder HTTP, usamos logs.
        print(f"[BACKGROUND ERROR] Error al conectar con MinIO en el lote {batch_id}: {e}")
    except Exception as e:
        print(f" [BACKGROUND ERROR] Error inesperado procesando el lote {batch_id}: {e}")

    

@app.post("/upload")
async def upload_pdf(file: UploadFile = File(...), background_tasks: BackgroundTasks = BackgroundTasks()):

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
        contenido = await file.read()  #lee el binario
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

        #1. Este id nos va a permitir idnetificar el zip
        #batch_id = str(uuid.uuid4())
        batch_id = f"batch-{uuid.uuid4().hex[:8]}"
        ruta_zip_minio = f"inputs/{batch_id}.zip"

        #2. Leemos el archivo para mandarlo a minio
        contenido_zip = await file.read()
        zip_buffer = io.BytesIO(contenido_zip)
        length = len(contenido_zip)
        
        # 3. Lo subimos a MinIO 
        minio_client.put_object(
            BUCKET, 
            ruta_zip_minio, 
            zip_buffer, 
            length, 
            content_type="application/zip"
        )

        jobs = []

        #1. Inicialmente recorremos el zip para crear la lista con los jobs
        with zipfile.ZipFile(zip_buffer, "r") as zip_ref:
            
            #Obtenemos solo los archivos .pdf del zip
            pdf_infos = [info for info in zip_ref.infolist() if not info.is_dir() and info.filename.lower().endswith(".pdf")]

            if not pdf_infos:
                raise HTTPException(
                    status_code=400,
                    detail="El ZIP no contiene archivos PDF"
                )

            #usamos infolist pq el zip puede tener adentro otras carpetas entonces si lanzamos la excepcion teniendo en cuenta que algun archivo dentro del zip no es .pdf no me procesa los archivos que si son .pdf
            for info in pdf_infos:
                
                job_id = str(uuid.uuid4())

                original_name = os.path.basename(info.filename)
                
                jobs.append({
                    "job_id": job_id,
                    "original_name": original_name,
                    "path": info.filename
                })

        # esto nos permite que ya no se vea elprocesamientp de manera secuencial 
        # El usuario2 no tiene que esperar que se termine de procesar el zip del usuario1 para ver el avance de su conversion
        # Se realiza la tarea pesada de decomprimir, subidad de pdfs individuales y envio a cola valkey sin interferir con nuevas peticiones de otros usuarios

        redis_client.set(f"batch:{batch_id}", json.dumps([job["job_id"] for job in jobs]))

        for job in jobs:
            redis_client.set(f"job_batch:{job['job_id']}", batch_id)
            redis_client.set(f"estado:{job['job_id']}", "Pendiente")
            

        background_tasks.add_task(procesar_zip_en_segundo_plano, ruta_zip_minio, batch_id, jobs)
        
        
        return {
            "batch_id": batch_id,
            "cantidad_pdfs": len(jobs)
        }
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

'''
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
'''

@app.get("/estado_zip/{batch_id}")
async def estado_zip(batch_id: str):

    try:
        obj = minio_client.get_object(
            BUCKET,
            f"batches/{batch_id}.json"
        )

        batch = json.loads(
        obj.read().decode("utf-8")
        )

        #batch = redis_client.get(f"batch:{batch_id}")
        #jobs = json.loads(batch)

        jobs = batch["jobs"]

        total = len(jobs)
        completados = 0
        errores = 0

        #Verificamos si valkey está disponible una sola vez
        valkey_disponible = True

        try:
            redis_client.ping()
        except Exception:
            valkey_disponible = False
            print("Valkey no disponible, usando solo MinIO como fuente de verdad")



        for job in jobs:

            job_id = job["job_id"]

            #---------------------------------------
            try:
                minio_client.stat_object(BUCKET, f"txt/{job_id}.txt")
                #si no lanza excepción, el archivo existe y su estado es completado
                estado = "Completado"
                #print(f"✅ {job_id}: TXT encontrado")
            
            except S3Error as e:
                #Si no esta en MinIO, consultamos Valkey solo si esta disponible
                if valkey_disponible:
                    estado = redis_client.get(f"estado:{job_id}")
                    if estado and estado.startswith("error"):
                        errores += 1
                        continue
                # Si Valkey no está o no tiene estado, el job sigue procesándose
                #print(f"❌ {job_id}: TXT NO encontrado - {e}")
                estado = "Pendiente"
                        
            if estado == "Completado":
                completados += 1

            elif estado and estado.startswith("error"):
                errores += 1

        if completados + errores == total:

            return {
                "estado": "Completado",
                "completados": completados,
                "errores": errores,
                "total": total
            }

        return {
            "estado": "Procesando",
            "completados": completados,
            "errores": errores,
            "total": total
        }


    except S3Error:
        raise HTTPException(
            status_code=404,
            detail="Lote no encontrado"
        )

    

    

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

@app.get("/download_zip/{batch_id}")
async def download_zip(batch_id: str):

    #batch = redis_client.get(f"batch:{batch_id}")

    obj = minio_client.get_object(
        BUCKET,
        f"batches/{batch_id}.json"
    )

    batch = json.loads(
        obj.read().decode("utf-8")
    )

    # Si es pendiente es porque se cayo el back y no se encolaron todos los jobs
    """if batch["estado"] == "Pendiente":
        raise HTTPException(
            status_code=409,
            detail="El zip no fue procesado completamente"
        )"""

    #jobs_data = json.loads(batch)
    jobs = batch["jobs"]

    zip_buffer = io.BytesIO()

    with zipfile.ZipFile(zip_buffer, "w") as zipf:

        for item in jobs:

            job_id = item["job_id"]
            original_name = item["original_name"]

            name = os.path.splitext(original_name)[0] + ".txt"

            try:
                obj = minio_client.get_object(
                    BUCKET,
                    f"txt/{job_id}.txt"
                )

                contenido = obj.read()
                #obj.close()
                #obj.release_conn()

                zipf.writestr(name, contenido)

            except S3Error: #en caso de que el archivo sea un pdf no extraible o valkey este caido
                estado = redis_client.get(f"estado:{job_id}")
                if not estado:
                    estado = "error_desconocido"
                    zipf.writestr(name, f"ERROR: {estado}")
    
    zip_buffer.seek(0)

    return StreamingResponse(
        zip_buffer,
        media_type="application/zip",
        headers={
            "Content-Disposition":
            f'attachment; filename="resultados.zip"'
        }
    )

@app.websocket("/ws/{job_id}")
async def websocket_status(websocket: WebSocket, job_id: str):
    await websocket.accept()

    # mando el estado actual por si ya estaba procesando
    status = redis_client.get(f"estado:{job_id}")
    await websocket.send_json({"estado": status or "Pendiente"})

    # Nos suscribimos al canal de ese job
    pubsub = redis_client.pubsub()
    pubsub.subscribe(f"job:{job_id}")

    # Esperamos el mensaje del worker
    loop = asyncio.get_event_loop()
    while True:
        mensaje = await loop.run_in_executor(None, pubsub.get_message, True, 1.0)
        if mensaje and mensaje["type"] == "message":
            estado = mensaje["data"]
            await websocket.send_json({"estado": estado})
            if estado == "Completado" or estado.startswith("error"):
                break

    pubsub.unsubscribe(f"job:{job_id}")
    await websocket.close()

@app.websocket("/ws/batch/{batch_id}")
async def websocket_batch(websocket: WebSocket, batch_id: str):
    await websocket.accept()

    batch = redis_client.get(f"batch:{batch_id}")
    if not batch:
        await websocket.send_json({"estado": "error", "mensaje": "Lote no encontrado"})
        await websocket.close()
        return

    jobs = json.loads(batch)
    total = len(jobs)

    # Contamos los que ya estaban completados antes de conectar
    completados = sum(1 for job_id in jobs if redis_client.get(f"estado:{job_id}") == "Completado")
    errores = sum(1 for job_id in jobs if redis_client.get(f"estado:{job_id}") and redis_client.get(f"estado:{job_id}").startswith("error"))

    # Mandamos estado inicial
    await websocket.send_json({
        "estado": "Completado" if completados + errores == total else "Procesando",
        "completados": completados,
        "errores": errores,
        "total": total
    })

    if completados + errores == total:
        await websocket.close()
        return

    # Suscribimos al canal del batch
    pubsub = redis_client.pubsub()
    pubsub.subscribe(f"batch:{batch_id}")

    loop = asyncio.get_event_loop()
    while True:
        mensaje = await loop.run_in_executor(None, pubsub.get_message, True, 1.0)
        if mensaje and mensaje["type"] == "message":
            # Llegó un job terminado, recalculamos
            completados = sum(1 for job_id in jobs if redis_client.get(f"estado:{job_id}") == "Completado")
            errores = sum(1 for job_id in jobs if redis_client.get(f"estado:{job_id}") and redis_client.get(f"estado:{job_id}").startswith("error"))

            await websocket.send_json({
                "estado": "Completado" if completados + errores == total else "Procesando",
                "completados": completados,
                "errores": errores,
                "total": total
            })

            if completados + errores == total:
                break

    pubsub.unsubscribe(f"batch:{batch_id}")
    await websocket.close()

    

'''
## web socket para comunicacion con el cliente cuando es un pdf
@app.websocket("/ws/{job_id}")
async def websocket_status(websocket: WebSocket, job_id: str):
    await websocket.accept()
    while True:
        try:
            minio_client.stat_object(BUCKET, f"txt/{job_id}.txt")
            redis_client.set(f"estado:{job_id}", "Completado")
            await websocket.send_json({"estado": "Completado"})
            break  # estado final, cerramos
        except S3Error:
            pass

        status = redis_client.get(f"estado:{job_id}")
        if status and status.startswith("error"):
            await websocket.send_json({"estado": status})
            break
        elif status:
            await websocket.send_json({"estado": status})
        else:
            await websocket.send_json({"estado": "Pendiente"})

        await asyncio.sleep(2)

    await websocket.close()

# webSocket cuando es un zip
@app.websocket("/ws/batch/{batch_id}")
async def websocket_batch(websocket: WebSocket, batch_id: str):
    await websocket.accept()
    while True:
        batch = redis_client.get(f"batch:{batch_id}")
        if not batch:
            await websocket.send_json({"estado": "error", "mensaje": "Lote no encontrado"})
            break

        jobs = json.loads(batch)
        total = len(jobs)
        completados = 0
        errores = 0

        for job_id in jobs:
            estado = redis_client.get(f"estado:{job_id}")
            if estado == "Completado":
                completados += 1
            elif estado and estado.startswith("error"):
                errores += 1

        await websocket.send_json({
            "estado": "Procesando" if completados + errores < total else "Completado",
            "completados": completados,
            "errores": errores,
            "total": total
        })

        if completados + errores == total:
            break

        await asyncio.sleep(2)

    await websocket.close()
'''
