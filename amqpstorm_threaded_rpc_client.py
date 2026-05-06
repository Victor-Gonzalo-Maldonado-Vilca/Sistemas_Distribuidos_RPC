import threading
import os
from time import sleep

from flask import Flask
import amqpstorm
from amqpstorm import Message

app = Flask(__name__)

# ── URL de CloudAMQP ────────────────────────────────────────────────────────
URL_NUBE = "amqps://vdhlnbov:ogNW_b6xOVvycixvXj2uyAJOpWgCbmkx@chameleon.lmq.cloudamqp.com/vdhlnbov"


class RpcClient(object):
    """Cliente RPC Asíncrono con reconexión automática."""

    def __init__(self, rpc_queue):
        self.queue = {}
        self.rpc_queue = rpc_queue
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self._lock = threading.Lock()
        self.open()

    def open(self):
        """Abre la conexión a CloudAMQP."""
        print("[Flask-RPC] Conectando a CloudAMQP...")
        self.connection = amqpstorm.UriConnection(URL_NUBE)
        self.channel = self.connection.channel()

        # Declarar la cola principal
        self.channel.queue.declare(self.rpc_queue)

        # Cola de respuestas exclusiva (se elimina al desconectarse)
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']

        # Consumir respuestas en esta cola
        self.channel.basic.consume(
            self._on_response,
            no_ack=True,
            queue=self.callback_queue
        )

        self._create_process_thread()
        print(f"[Flask-RPC] Conectado. Cola de respuestas: {self.callback_queue}")

    def _create_process_thread(self):
        """Hilo dedicado a procesar eventos de respuesta."""
        thread = threading.Thread(target=self._process_data_events)
        thread.daemon = True
        thread.start()

    def _process_data_events(self):
        """Consume mensajes de la cola de respuestas de forma continua."""
        self.channel.start_consuming(to_tuple=False)

    def _on_response(self, message):
        """Guarda la respuesta en el diccionario usando el correlation_id."""
        with self._lock:
            self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        """Envía un mensaje RPC y retorna el correlation_id."""
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue

        with self._lock:
            self.queue[message.correlation_id] = None

        message.publish(routing_key=self.rpc_queue)
        print(f"[Flask-RPC] Mensaje enviado. corr_id={message.correlation_id}")
        return message.correlation_id


# ── Inicialización LAZY del cliente RPC ────────────────────────────────────
# Se inicializa al primer request, no al arrancar el proceso.
# Esto evita errores de conexión durante el arranque en Render.
_rpc_client = None
_rpc_lock = threading.Lock()

def get_rpc_client():
    global _rpc_client
    if _rpc_client is None:
        with _rpc_lock:
            if _rpc_client is None:
                _rpc_client = RpcClient('rpc_queue')
    return _rpc_client


# ── Rutas Flask ─────────────────────────────────────────────────────────────

@app.route('/')
def index():
    return (
        "<h2>Servidor RPC Flask activo ✅</h2>"
        "<p>Usa <code>/rpc_call/tu_mensaje</code> para enviar una solicitud.</p>"
    )

@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    """Envía un mensaje al Worker vía RabbitMQ y espera la respuesta."""
    print(f"[Flask-RPC] Nueva solicitud: {payload}")

    try:
        client = get_rpc_client()
    except Exception as e:
        print(f"[Flask-RPC] Error al conectar RPC Client: {e}")
        return f"Error de conexión con RabbitMQ: {e}", 503

    corr_id = client.send_request(payload)

    # Esperar hasta 30 segundos (300 × 0.1 s)
    intentos = 0
    MAX_INTENTOS = 300
    while True:
        with client._lock:
            valor = client.queue.get(corr_id)
        if valor is not None:
            break
        if intentos >= MAX_INTENTOS:
            print(f"[Flask-RPC] TIMEOUT para corr_id={corr_id}")
            with client._lock:
                client.queue.pop(corr_id, None)
            return "Error: El Worker no respondió a tiempo (Timeout)", 504
        sleep(0.1)
        intentos += 1

    with client._lock:
        respuesta = client.queue.pop(corr_id)

    print(f"[Flask-RPC] Respuesta recibida: {respuesta}")
    return respuesta


# ── Arranque ────────────────────────────────────────────────────────────────
if __name__ == '__main__':
    port = int(os.environ.get("PORT", 5000))
    app.run(host='0.0.0.0', port=port, debug=False)