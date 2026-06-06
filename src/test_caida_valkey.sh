#!/bin/bash

echo "Iniciando monitor de crash para Valkey..."

while true
do
    # Ejecutamos valkey-cli dentro del contenedor desde afuera
    # Nota: Usamos valkey-cli ya que estás usando la imagen de Valkey
    LAG=$(sudo docker compose exec -T valkey valkey-cli XINFO GROUPS trabajos \
      | grep -A1 '"lag"' \
      | tail -n1 \
      | tr -dc '0-9')

    # Limpiamos el output de XPENDING para quedarnos solo con el número entero
    PENDING=$(sudo docker compose exec -T valkey valkey-cli XPENDING trabajos workers \
      | head -n1 \
      | tr -dc '0-9')

    # Si las variables quedaron vacías por un segundo, les asignamos 0 para evitar errores
    [ -z "$LAG" ] && LAG=0
    [ -z "$PENDING" ] && PENDING=0

    echo "Estado actual -> lag=$LAG | pending=$PENDING"

    # Condición: No hay más tareas nuevas en cola, pero hay tareas sin terminar (procesándose)
    if [ "$LAG" = "0" ] && [ "$PENDING" -gt 0 ]
    then
        echo "¡CONDICIÓN CUMPLIDA! Forzando apagado de Valkey ahora mismo..."
        sudo docker compose kill valkey
        break
    fi

    sleep 1
done
