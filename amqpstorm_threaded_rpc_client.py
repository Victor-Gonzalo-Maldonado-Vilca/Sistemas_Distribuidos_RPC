import threading
from time import sleep
from flask import Flask
import amqpstorm
from amqpstorm import Message
import os

app = Flask(__name__)

class RpcClient(object):
    def __init__(self, rpc_queue):
        self.queue = {}
        self.rpc_queue = rpc_queue
        self.channel = None
        self.connection = None
        self.callback_queue = None
        self.open()

    def open(self):
        URL_NUBE = "amqps://vdhlnbov:ogNW_b6xOVvycixvXj2uyAJOpWgCbmkx@chameleon.lmq.cloudamqp.com/vdhlnbov"
        # Agregamos heartbeat=60 para mantener la conexión viva
        self.connection = amqpstorm.UriConnection(URL_NUBE, heartbeat=60)
        self.channel = self.connection.channel()
        self.channel.queue.declare(self.rpc_queue)
        result = self.channel.queue.declare(exclusive=True)
        self.callback_queue = result['queue']
        self.channel.basic.consume(self._on_response, no_ack=True, queue=self.callback_queue)
        self._create_process_thread()

    def _create_process_thread(self):
        thread = threading.Thread(target=self._process_data_events)
        thread.daemon = True # Corregido: daemon en lugar de setDaemon
        thread.start()

    def _process_data_events(self):
        try:
            self.channel.start_consuming(to_tuple=False)
        except Exception:
            pass

    def _on_response(self, message):
        self.queue[message.correlation_id] = message.body

    def send_request(self, payload):
        # Verificar si la conexión sigue viva antes de enviar
        if not self.connection or self.connection.is_closed:
            self.open()
            
        message = Message.create(self.channel, payload)
        message.reply_to = self.callback_queue
        self.queue[message.correlation_id] = None
        message.publish(routing_key=self.rpc_queue)
        return message.correlation_id

# IMPORTANTE: Creamos el cliente
RPC_CLIENT = RpcClient('rpc_queue')

@app.route('/')
def index():
    return "Servidor RPC Flask encendido. Usa /rpc_call/tu_mensaje"

@app.route('/rpc_call/<payload>')
def rpc_call(payload):
    corr_id = RPC_CLIENT.send_request(payload)
    
    # Aumentamos el tiempo de espera a 15 segundos (150 * 0.1)
    intentos = 0
    while RPC_CLIENT.queue.get(corr_id) is None and intentos < 150:
        sleep(0.1)
        intentos += 1

    response = RPC_CLIENT.queue.pop(corr_id, None)
    if response is None:
        return "Error: El Worker no respondió (Timeout)", 504
    
    # Decodificar si es bytes
    if isinstance(response, bytes):
        return response.decode()
    return response

if __name__ == '__main__':
    app.run()