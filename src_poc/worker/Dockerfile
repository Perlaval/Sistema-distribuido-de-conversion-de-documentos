# Imagen base de Python liviana
FROM python:3.11-slim

# Directorio de trabajo dentro del contenedor
WORKDIR /app

# Copiamos primero el requirements para aprovechar el cache de Docker
# Si no cambia el requirements, Docker no reinstala las dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copiamos el código del worker
COPY worker.py .

# Comando que se ejecuta cuando arranca el contenedor
CMD ["python", "worker.py"]