

// Variables globales
var pollingInterval = null;
var currentJobId = null;
var currentBatchId = null;

// Función que actualiza el UI
function updateUI(status, message) { 
    var labels = {
    'En cola':    'En cola',
    'Procesando': 'Procesando PDF',
    'Completado': 'Conversión completada',
    'Tarea no encontrada': 'Tarea no encontrada',
    'error_pdf_corrupto': 'No se pudo leer el PDF',
    'error': 'Ocurrió un error durante la conversión',
    'error_pdf_sin_texto': 'Pdf sin texto'
};

  /*$('#status-msg')
    .text(labels[status] || message)
    .attr('class', 'status-badge status-' + status);*/

   $('#status-msg')
        .text(labels[status] || message)
        .attr('class', 'status-card'); 
}

function startWebSocket(uuid) {
    //var ws = new WebSocket('ws://127.0.0.1:8000/ws/' + uuid);

    var ws = new WebSocket('ws://' + window.location.host + '/ws/' + uuid);

    ws.onmessage = function(event) {
        var data = JSON.parse(event.data);
        updateUI(data.estado, data.estado);

        // agrego para poder descargar en el front
        if (data.estado === 'Completado') {
          $("#btn-descargar")
            .attr(
                "href",
                //"http://127.0.0.1:8000/download/" + currentJobId
                "/api/download/" + currentJobId
            )
            .show();
        }
        
        if (data.estado === 'Completado') {

        $("#btn-descargar")
            .attr("href", data.url)
            .show();
    }

        if (data.estado === 'Completado' || data.estado.startsWith('error')) {
            ws.close();  //
        }
    };

    ws.onerror = function() {
        updateUI('error', 'Error de conexión');
    };
}


// El POST inicial + llamada a startPolling
$(document).ready(function() {

    //Recuperamos el batch pendiente si existe
    var batchGuardado = localStorage.getItem("ultimo_batch");
    if(batchGuardado) {
        // Verificamos si el batch ya está completado antes de retomar
        $.get("/api/estado_zip/" + batchGuardado, function(data) {
            if(data.estado === "Completado") {
                // Ya terminó, limpiar localStorage
                localStorage.removeItem("ultimo_batch");
                $("#status-msg").text("Conversión anterior completada.");
                $("#btn-descargar")
                    .attr("href", "/api/download_zip/" + batchGuardado)
                    .show();
            } else {
                // Sigue en proceso, retomar polling
                currentBatchId = batchGuardado;
                $("#status-msg").text("Retomando conversión anterior...");
                startBatchPolling(batchGuardado);
            }
        }).fail(function() {
            // Backend no disponible, limpiar localStorage
            localStorage.removeItem("ultimo_batch");
            $("#status-msg").text("No se pudo retomar la conversión anterior.");
        });
    }

  $('#btn-convertir').on('click', function() {
    $.ajax({
      //url: 'http://127.0.0.1:8000/upload',  
      url:         '/api/upload',
      method:      'POST',
      data:        new FormData($('#mi-form')[0]),
      contentType: false,
      processData: false,

      
      /*success: function(response) {
        currentJobId = response.job_id;
        startWebSocket(response.job_id);
      }*/

      success: function(response) {

        // Caso PDF individual
        if(response.job_id){
            currentJobId = response.job_id;
            startWebSocket(response.job_id);
        }

        // Caso ZIP
        else if(response.batch_id){

            currentBatchId = response.batch_id;

            // Guardar en localStorage para recuperar si se pierde
            localStorage.setItem("ultimo_batch", response.batch_id);
            //localStorage.setItem("ultimo_batch_total", response.cantidad_pdfs);

            $("#status-msg").text(
                "Zip recibido. PDFs a convertir: " +
                response.cantidad_pdfs 
            );
            
            startBatchPolling(currentBatchId);

            //response.jobs.forEach(jobId => {
            //    startWebSocket(jobId);
            //});
        }
      },

      error: function(xhr) {
            console.error(xhr.responseText);

            $("#status-msg").text(
                "Error al subir el archivo"
            );
        }

    });
  });

});

// para el estado del zip
/*function startBatchPolling(batchId) {

    const interval = setInterval(function () {

        $.get(
            "http://127.0.0.1:8000/estado_zip/" + batchId,

            function(data) {
                console.log("Respuesta batch:", data);
                $("#status-msg").text(
                    "Procesados " +
                    data.completados +
                    " de " +
                    data.total +
                    " PDFs"
                );

                if (data.estado === "Completado") {

                    clearInterval(interval);
                    localStorage.removeItem("ultimo_batch");

                    if (data.errores > 0) {

                        $("#status-msg").text(
                            "Conversión finalizada. " +
                            data.completados + 
                            " archivos convertidos, " + "\n" +
                            data.errores +
                            " con error."
                        ); 
                        
                    } else { 

                        $("#status-msg").text(
                            "Conversión completada"
                        );
                    }

                    $("#btn-descargar")
                        .attr(
                            "href",
                            "http://127.0.0.1:8000/download_zip/" +
                            batchId
                        )
                        .show();
                }
            }
        );

    }, 2000);
}*/

function startBatchPolling(batchId) {
    var reintentosFallidos = 0;
    const max_reintentos = 5;

    //var TIMEOUT = 60 * 1000; //1 minuto
    //localStorage.setItem("polling_inicio", Date.now());

    const interval = setInterval(function () {

        $.ajax({
            //url: "http://127.0.0.1:8000/estado_zip/" + batchId,
            url: "/api/estado_zip/" + batchId,
            method: "GET"
        })
        .done(function(data) {
            //var inicio = parseInt(localStorage.getItem("polling_inicio"));
            //var tiempoEsperado = Date.now() - inicio;
            
            console.log("Respuesta batch:", data);
            reintentosFallidos = 0; // caso exito: reiniciamos la variable

            $("#status-msg").text(
                "Procesados " +
                data.completados +
                " de " +
                data.total +
                " PDFs"
            );
            
            // Agregamos esto para detener el polling si se supera el tiempo de espera - asi el front no se queda esperando infinitamente 
            // si se perdio el batch o se cayo un servicio 
            /*if (data.estado == "Procesando" && tiempoEsperado > TIMEOUT){
                clearInterval(interval);
                localStorage.removeItem("ultimo_batch");
                localStorage.removeItem("polling_inicio");
                $("#status-msg").text("Tiempo de espera agotado. Recargá para reintentar.");
                return;
            }*/

            if (data.estado === "Completado") {
                clearInterval(interval);
                localStorage.removeItem("ultimo_batch");

                if (data.errores > 0) {
                    $("#status-msg").text(
                        "Conversión finalizada. " +
                        data.completados + 
                        " archivos convertidos, " + "\n" +
                        data.errores +
                        " con error."
                    ); 
                } else { 
                    $("#status-msg").text(
                        "Conversión completada"
                    );
                }

                $("#btn-descargar")
                    .attr(
                        "href",
                        //"http://127.0.0.1:8000/download_zip/" + batchId
                        "/api/download_zip/" + batchId
                    )
                    .show();
                
                // Rehabilitamos el botón por si quiere subir otro
                $('#btn-convertir').prop('disabled', false).text('Convertir ZIP');
            }


        })
        .fail(function() { //xhr

            /*if (xhr.status === 404) {
                console.warn("El lote no existe en el servidor (404). Limpiando LocalStorage.");
                
                clearInterval(interval); // 1. Matamos el bucle inmediatamente
                localStorage.removeItem("ultimo_batch"); // 2. Borramos el ID fantasma de la memoria
                
                // 3. Avisamos en pantalla de forma amigable
                $("#status-msg").text("La sesión anterior expiró o fue eliminada.");
                $('#btn-convertir').prop('disabled', false).text('Convertir ZIP');
            } else {
                // Si es un error 500 o caída de red, no hacemos nada y dejamos que intente en 2 segundos
                console.error("Error temporal del servidor, reintentando código:", xhr.status);
            }*/

            reintentosFallidos++;
            console.log('Backend no disponible. Reintento ${reintentosFallidos}/${max_reintentos}');
            $("#status-msg").text("Reconectando...");

            if (reintentosFallidos >= max_reintentos){
                clearInterval(interval);
                localStorage.removeItem("ultimo_batch");
                $("#status-msg").text("No se pudo conectar al servidor. Recargá la página para reintentar.");
                console.log("Demasiados reintentos, deteniendo polling");
            }

        });

    }, 2000);
}

$("#file").on("change", function () {
    const archivo = this.files[0];

    if (archivo) {

        if (archivo.name.endsWith(".zip")) {
            $("#file-name").text(" " + archivo.name);
        } else {
            $("#file-name").text(" " + archivo.name);
        }
    }
});


/* Función que actualiza el UI
function updateUI(status, message) { 
    var labels = {
        'En cola':    'En cola...',
        'Procesando': 'Procesando archivos...',
        'Completado': '¡Conversión completada!',
        'error':      'Error en el servidor',
        'Tarea no encontrada': 'Tarea no encontrada'
    };

    // Aseguramos un string limpio para la clase CSS (ej: "error" si empieza con "error: ...")
    var classStatus = status.startsWith('error') ? 'error' : status;

    $('#status-msg')
        .text(labels[status] || message)
        .attr('class', 'status-badge status-' + classStatus);
}

function startWebSocket(uuid) {
    // Cambiamos a la ruta correcta de tu backend: /ws/estado_zip/{batch_id}
    var ws = new WebSocket('ws://127.0.0.1:8000/ws/estado_zip/' + uuid);

    ws.onopen = function() {
        console.log("Conexión WebSocket establecida para el lote:", uuid);
        updateUI('Procesando', 'Conectado, esperando actualización...');
    };

    ws.onmessage = function(event) {
        var data = JSON.parse(event.data);
        
        // Actualizamos la UI con los contadores que envía tu backend
        // Ejemplo de mensaje esperado: "Completados: 5 / 11"
        var mensajePersonalizado = `Procesando: ${data.completados} de ${data.total}`;
        updateUI(data.estado, mensajePersonalizado);

        if (data.estado === 'Completado') {
            // Unificamos el botón de descarga usando el UUID del lote
            $("#btn-descargar")
                .attr("href", "http://127.0.0.1:8000/download/" + uuid)
                .show();
            
            // Volvemos a habilitar el botón de convertir para un nuevo lote
            $('#btn-convertir').prop('disabled', false).text('Convertir otro ZIP');
            ws.close(); 
        }

        if (data.estado.startsWith('error') || data.estado === 'Tarea no encontrada') {
            $('#btn-convertir').prop('disabled', false);
            ws.close();
        }
    };

    ws.onerror = function(error) {
        console.error("Error en WebSocket:", error);
        updateUI('error', 'Error de conexión con el servidor.');
    };

    ws.onclose = function(event) {
        console.log("WebSocket cerrado. Código:", event.code);
        // Si se cierra antes de tiempo (y no es por éxito), alertamos al usuario
        // Aquí podrías meter un setTimeout para intentar reconectar si quieres
    };
}

$(document).ready(function() {
    
    $('#btn-convertir').on('click', function(e) {
        // 1. Evitamos que el botón haga cosas raras por defecto
        e.preventDefault();

        // 2. Buscamos el input del archivo dentro de tu formulario
        var fileInput = $('#mi-form').find('input[type="file"]')[0];
        
        // 3. ¡VALIDACIÓN CRÍTICA! Si el usuario no eligió nada, frenamos aquí
        if (!fileInput || fileInput.files.length === 0) {
            alert("Por favor, selecciona un archivo primero.");
            return; 
        }

        // 4. CAMBIO DE ORDEN: Primero preparamos los textos de la UI...
        $("#btn-descargar").hide(); 
        updateUI('En cola', 'Subiendo archivo ZIP...');
        
        // ...pero NO deshabilitamos el botón todavía. Lo dejamos habilitado
        // hasta que el AJAX tome los datos para que el navegador no congele el archivo.
        $('#btn-convertir').text('Subiendo...');

        // 5. Tu AJAX intacto y directo
        $.ajax({
            url: 'http://127.0.0.1:8000/upload',  
            method: 'POST',
            data: new FormData($('#mi-form')[0]),
            contentType: false,
            processData: false,
            success: function(response) {
                // Ahora que ya se subió con éxito, bloqueamos el botón para que no mande otro
                $('#btn-convertir').prop('disabled', true);
                
                var id = response.batch_id || response.job_id || response.id;
                console.log("Subida exitosa. Conectando al WebSocket para el ID:", id);
                
                startWebSocket(id);
            },
            error: function() {
                updateUI('error', 'No se pudo subir el archivo.');
                $('#btn-convertir').prop('disabled', false).text('Convertir ZIP');
            }
        });
    });
});*/