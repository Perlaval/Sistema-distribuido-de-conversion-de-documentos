#import json No pq vamos a usar Redis Stream 
import os
from minio import Minio
from minio.error import S3Error
import redis
from pdfminer.high_level import extract_text
import tempfile

# Configuración desde variables de entorno------------------------------------------------------------

VALKEY_HOST = os.getenv("VALKEY_HOST", "my-valkey-cluster")
VALKEY_PORT = int(os.getenv("VALKEY_PORT", 6379))
VALKEY_PASSWORD = os.getenv("VALKEY_PASSWORD", "")

MINIO_HOST = os.getenv("MINIO_HOST", "myminio")
MINIO_ACCESS_KEY = os.getenv("MINIO_ACCESS_KEY")
MINIO_SECRET_KEY = os.getenv("MINIO_SECRET_KEY")
BUCKET = "documentos" # nombre del bucket en MinIO donde van PDFs y TXTs
STREAM = "trabajos" # nombre del stream en Valkey donde el Backend publica mensajes
GROUP = "workers" # nombre del grupo de consumidores, permite que varios workers 
                    #lean el stream sin procesar el mismo mensaje dos veces
CONSUMER = os.getenv("HOSTNAME", "worker-1")  # cada pod tiene su propio hostname

# Conexiones ------------------------------------------------------------------------------------------

r = redis.Redis(host=VALKEY_HOST, port=VALKEY_PORT, password=VALKEY_PASSWORD, decode_responses=True)
minio_client = Minio(MINIO_HOST, access_key=MINIO_ACCESS_KEY, secret_key=MINIO_SECRET_KEY, secure=False)

#Inicializacion del stream ----------------------------------------------------------------------------
def inicializar_stream():

    # Creamos el grupo de consumidores si no existe
    try:
        r.xgroup_create(STREAM, GROUP, id="0", mkstream=True)
        print(f"Grupo '{GROUP}' creado en stream '{STREAM}'")
    except redis.exceptions.ResponseError as e:
        if "BUSYGROUP" in str(e):
            # si el worker se reinicia pq se cae el pod, Valkey va a responder con este error "el grupo ya existe" entonces no es un error error
            print(f"Grupo '{GROUP}' ya existe, continuando...")
        else:
            raise e

#Recibir Msg ------------------------------------------------------------------------------------------
def iniciar_worker():
    print("Worker iniciado, esperando mensajes...")
    while True:
        # Leemos el mensaje del stream, espera hasta 5 segundos si no hay mensajes
        mensajes = r.xreadgroup(GROUP, CONSUMER, {STREAM: ">"}, count=1, block=5000)

        if not mensajes:
            continue  # seguimos esperando hasta que llegue un mensaje

        for stream, lista_mensajes in mensajes:
        # stream = "trabajos"
        # lista_mensajes = [("1717123456789-0", {"job_id": "abc123", "file_path": "pdfs/abc123.pdf"})]
            for msg_id, datos in lista_mensajes:
                # msg_id = "1717123456789-0" 
                # datos  = {"job_id": "abc123", "file_path": "pdfs/abc123.pdf"}
                job_id = datos["job_id"]
                file_path = datos["file_path"]
                print(f"Procesando job {job_id}")

                procesar_mensaje(job_id, file_path)

                # Confirmamos que el mensaje fue procesado correctamente
                r.xack(STREAM, GROUP, msg_id)
                # le dice a Valkey que este mensaje fue procesado exitosamente
                # si el worker se cae antes del xack, el mensaje queda disponible para reintento

def extraer_texto(pdf_path, job_id):
    try:
        texto = extract_text(pdf_path)
        if not texto.strip():
            # el PDF se leyó pero no tenía texto (ej: PDF escaneado como imagen)
            r.set(f"estado:{job_id}", "error_pdf_sin_texto")
            print(f"Job {job_id}: PDF sin texto extraíble")
            return None
        return texto
    except Exception as e:
        r.set(f"estado:{job_id}", "error_pdf_corrupto")
        print(f"Job {job_id}: PDF corrupto o ilegible - {e}")
        return None

#Procesar Msg -----------------------------------------------------------------------------------------
def procesar_mensaje(job_id, file_path):
    try:

        # 1. Actualizamos el estado a "procesando" en Valkey
        r.set(f"estado:{job_id}", "procesando")

        # 2. Descargamos el PDF desde MinIO a un archivo temporal
        # porque pdfminer necesita leer el pdf desde el disco local
        with tempfile.NamedTemporaryFile(suffix=".pdf", delete=False) as tmp_pdf:
            # crea un archivo temporal en el disco local del Worker
            # tmp_pdf.name: ruta del archivo temporal.
            minio_client.fget_object(BUCKET, file_path, tmp_pdf.name)
            pdf_path = tmp_pdf.name

        # 3. Extraemos el texto del PDF con esta funcion de pdfminer.six
        texto = extraer_texto(pdf_path, job_id)
        if texto is None:
            return  # no seguimos procesando, el estado ya fue actualizado

        # 4. Subimos el .TXT resultante a MinIO
        txt_path = f"txt/{job_id}.txt"
        with tempfile.NamedTemporaryFile(suffix=".txt", delete=False, mode="w") as tmp_txt:
            tmp_txt.write(texto)
            tmp_txt_path = tmp_txt.name

        minio_client.fput_object(BUCKET, txt_path, tmp_txt_path)

        # 5. Actualizamos el estado a "completado" en Valkey
        r.set(f"estado:{job_id}", "completado")
        print(f"Job {job_id} completado")

    except Exception as e:
        r.set(f"estado:{job_id}", "error")
        print(f"Error procesando job {job_id}: {e}")
        raise e



#Main -------------------------------------------------------------------------------------------------
if __name__ == "__main__":
    # Verificamos que el bucket existe, si no, lo creamos
    if not minio_client.bucket_exists(BUCKET):
        minio_client.make_bucket(BUCKET)
        print(f"Bucket '{BUCKET}' creado")
    else:
        print(f"Bucketj '{BUCKET}' ya existe")

    # Inicializar el stream y iniciar el worker.
    inicializar_stream()
    iniciar_worker()