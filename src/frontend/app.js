

// Variables globales
var pollingInterval = null;
var currentJobId = null;


// Función que actualiza el UI
function updateUI(status, message) { 
    var labels = {
    'En cola':    'En cola',
    'Procesando': 'Procesando PDF',
    'Completado': 'Conversión completada',
    'Tarea no encontrada': 'Tarea no encontrada'
};

  $('#status-msg')
    .text(labels[status] || message)
    .attr('class', 'status-badge status-' + status);
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

      success: function(response) {
        currentJobId = response.job_id;
        startWebSocket(response.job_id);
      }
    });
  });

});


$("#file").on("change", function () {
    const archivo = this.files[0];

    if (archivo) {
        $("#file-name").text(archivo.name);
    }
});

