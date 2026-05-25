# 1. Imagen base de Python (Liviana)
FROM python:3.9-slim

# 2. Directorio de trabajo dentro del contenedor
WORKDIR /app

# 3. Copiar y cargar dependencias
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 4. Copiar el código de la aplicación
COPY main.py .

# 5. Exponer el puerto que usará FastAPI
EXPOSE 8000

# 6. Comando para iniciar el servidor Uvicorn
CMD ["uvicorn", "main:app", "--host", "0.0.0.0", "--port", "8000"]