

// Variables globales
var pollingInterval = null;


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

        if (data.estado === 'Completado' || data.estado === 'Tarea no encontrada') {
            ws.close();  //
        }
    };

    ws.onerror = function() {
        updateUI('error', 'Error de conexión');
    };
}

/*
function startPolling(uuid) {
  pollingInterval = setInterval(function() {

    $.ajax({
      url: 'http://127.0.0.1:8000/estado/' + uuid,
     // url:      '/estado/' + uuid,
      method:   'GET',
      dataType: 'json',

      success: function(data) {
        updateUI(data.status, data.message);

        // Detener al llegar a un estado final
        if (data.status === 'done' || data.status === 'error') {
          clearInterval(pollingInterval);
        }
      },

      error: function(xhr, textStatus) {
        clearInterval(pollingInterval);
        updateUI('error', 'Error de conexión: ' + textStatus);
      }
    });

  }, 2000);
}*/

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
        startWebSocket(response.job_id);
      }
    });
  });

});