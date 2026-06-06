

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
/*
  $('#status-msg')
    .text(labels[status] || message)
    .attr('class', 'status-badge status-' + status);*/

   $('#status-msg')
        .text(labels[status] || message)
        .attr('class', 'status-card'); 
}

function startWebSocket(uuid) {
    var ws = new WebSocket('ws://127.0.0.1:8000/ws/' + uuid);

    ws.onmessage = function(event) {
        var data = JSON.parse(event.data);
        updateUI(data.estado, data.estado);

        // agrego para poder descargar en el front
        if (data.estado === 'Completado') {
          $("#btn-descargar")
            .attr(
                "href",
                "http://127.0.0.1:8000/download/" + currentJobId
            )
            .show();
        }
        /*
        if (data.estado === 'Completado') {

        $("#btn-descargar")
            .attr("href", data.url)
            .show();
    }*/

        if (data.estado === 'Completado' || data.estado === 'Tarea no encontrada') {
            ws.close();  //
        }
    };

    ws.onerror = function() {
        updateUI('error', 'Error de conexión');
    };
}


// El POST inicial + llamada a startPolling
$(document).ready(function() {
  $('#btn-convertir').on('click', function() {
    $.ajax({
      url: 'http://127.0.0.1:8000/upload',  
      //url:         '/upload',
      method:      'POST',
      data:        new FormData($('#mi-form')[0]),
      contentType: false,
      processData: false,

      /*
      success: function(response) {
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

            $("#status-msg").text(
                "Zip recibido. PDFs a convertir: " +
                response.cantidad_pdfs 
            );
            
            startBatchPolling(currentBatchId);

            //response.jobs.forEach(jobId => {
            //    startWebSocket(jobId);
            //});
        }
      }
    });
  });

});

// para el estado del zip
function startBatchPolling(batchId) {

    const interval = setInterval(function () {

        $.get(
            "http://127.0.0.1:8000/estado_zip/" + batchId,

            function(data) {

                $("#status-msg").text(
                    "Procesados " +
                    data.completados +
                    " de " +
                    data.total +
                    " PDFs"
                );

                if (data.estado === "Completado") {

                    clearInterval(interval);

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
