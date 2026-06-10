// Variables globales
var currentJobId   = null;
var currentBatchId = null;

// Actualiza el mensaje de estado en pantalla
function updateUI(mensaje) {
    $('#status-msg')
        .text(mensaje)
        .attr('class', 'status-card');
}

// WebSocket para PDF individual
function startWebSocket(jobId) {
    var ws = new WebSocket('ws://' + window.location.host + '/ws/' + jobId);

    ws.onopen = function() {
        updateUI('Conectando...');
    };

    ws.onmessage = function(event) {
        var data = JSON.parse(event.data);
        var labels = {
            'Pendiente':            'En cola...',
            'Procesando':           'Procesando PDF...',
            'Completado':           'Conversión completada',
            'Tarea no encontrada':  'Tarea no encontrada',
            'error_pdf_corrupto':   'No se pudo leer el PDF',
            'error_pdf_sin_texto':  'El PDF no contiene texto',
            'error':                'Ocurrió un error durante la conversión'
        };

        updateUI(labels[data.estado] || data.estado);

        if (data.estado === 'Completado') {
            $('#btn-descargar')
                .attr('href', '/api/download/' + currentJobId)
                .show();
            ws.close();
        }

        if (data.estado === 'Tarea no encontrada' || data.estado.startsWith('error')) {
            ws.close();
        }
    };

    ws.onerror = function() {
        updateUI('Error de conexión');
    };

    ws.onclose = function() {
        console.log('WebSocket PDF cerrado');
    };
}

// WebSocket para ZIP (batch)
function startBatchWebSocket(batchId) {
    var ws = new WebSocket('ws://' + window.location.host + '/ws/batch/' + batchId);

    ws.onopen = function() {
        updateUI('Conectando...');
    };

    ws.onmessage = function(event) {
        var data = JSON.parse(event.data);

        if (data.estado === 'Completado') {
            localStorage.removeItem('ultimo_batch');

            if (data.errores > 0) {
                updateUI(
                    'Finalizado: ' + data.completados + ' convertidos, ' + data.errores + ' con error.'
                );
            } else {
                updateUI('Conversión completada');
            }

            $('#btn-descargar')
                .attr('href', '/api/download_zip/' + currentBatchId)
                .show();

            ws.close();

        } else if (data.estado === 'error') {
            updateUI(data.mensaje || 'Error en el lote');
            localStorage.removeItem('ultimo_batch');
            ws.close();

        } else {
            updateUI('Procesados ' + data.completados + ' de ' + data.total + ' PDFs...');
        }
    };

    ws.onerror = function() {
        updateUI('Error de conexión');
    };

    ws.onclose = function() {
        console.log('WebSocket batch cerrado');
    };
}

// POST inicial al hacer click en Convertir
$(document).ready(function() {

    // Recuperar batch pendiente si se recargó la página
    var batchGuardado = localStorage.getItem('ultimo_batch');
    if (batchGuardado) {
        currentBatchId = batchGuardado;
        updateUI('Retomando conversión anterior...');
        startBatchWebSocket(batchGuardado);
    }

    $('#btn-convertir').on('click', function() {

        $('#btn-descargar').hide();
        updateUI('Subiendo archivo...');

        $.ajax({
            url:         '/api/upload',
            method:      'POST',
            data:        new FormData($('#mi-form')[0]),
            contentType: false,
            processData: false,

            success: function(response) {
                // PDF individual
                if (response.job_id) {
                    currentJobId = response.job_id;
                    startWebSocket(response.job_id);
                }
                // ZIP
                else if (response.batch_id) {
                    currentBatchId = response.batch_id;
                    localStorage.setItem('ultimo_batch', response.batch_id);
                    updateUI('ZIP recibido. PDFs a convertir: ' + response.cantidad_pdfs);
                    startBatchWebSocket(response.batch_id);
                }
            },

            error: function() {
                updateUI('Error al subir el archivo');
            }
        });
    });

    // Mostrar nombre del archivo seleccionado
    $('#file').on('change', function() {
        var archivo = this.files[0];
        if (archivo) {
            var icono = archivo.name.endsWith('.zip') ? '📦 ' : '📄 ';
            $('#file-name').text(icono + archivo.name);
        }
    });

});



















