import amqpstorm
import threading
import http.server
import socketserver
import os
import sys
import json
from datetime import datetime, timezone
 
sys.stdout.reconfigure(line_buffering=True)
 
# ── URL de CloudAMQP ────────────────────────────────────────────────────────
URL_NUBE = "amqps://vdhlnbov:ogNW_b6xOVvycixvXj2uyAJOpWgCbmkx@chameleon.lmq.cloudamqp.com/vdhlnbov"
 
 
# ── Servidor HTTP de mantenimiento ──────────────────────────────────────────
def run_dummy_server():
    PORT = int(os.environ.get("PORT", 10000))
    socketserver.TCPServer.allow_reuse_address = True
 
    class SilentHandler(http.server.SimpleHTTPRequestHandler):
        def log_message(self, format, *args):
            pass
 
        def do_GET(self):
            self.send_response(200)
            self.end_headers()
            self.wfile.write(b"Worker RPC activo")
 
    with socketserver.TCPServer(("", PORT), SilentHandler) as httpd:
        print(f" [!] Servidor de mantenimiento activo en puerto {PORT}")
        httpd.serve_forever()
 
threading.Thread(target=run_dummy_server, daemon=True).start()
 
 
# ── Lógica de procesamiento ─────────────────────────────────────────────────

def construir_respuesta(payload: str) -> str:
    """
    Construye una respuesta estructurada y formal para el cliente RPC.
    Retorna un texto con formato legible para navegador o terminal.
    """
    timestamp = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")

    resultado = payload.upper()

    respuesta = (
        f"=== RESPUESTA DEL WORKER RPC ===\n"
        f"\n"
        f"  Estado        : OK\n"
        f"  Timestamp     : {timestamp}\n"
        f"  Nodo Worker   : worker-rpc-01\n"
        f"  Cola origen   : rpc_queue\n"
        f"\n"
        f"  Mensaje recibido  : {payload}\n"
        f"  Resultado proceso : {resultado}\n"
        f"\n"
        f"  Descripción   : El Worker recibió el payload a través de la cola\n"
        f"                  RabbitMQ (AMQP), aplicó la transformación definida\n"
        f"                  y retornó la respuesta mediante la cola de retorno\n"
        f"                  exclusiva del cliente (reply_to).\n"
        f"\n"
        f"================================\n"
    )

    return respuesta


def procesar_pedido(message):
    """Procesa un mensaje de la rpc_queue y envía la respuesta al cliente."""
    try:
        payload = message.body
        if isinstance(payload, bytes):
            payload = payload.decode('utf-8')
 
        print(f" [x] Mensaje recibido: '{payload}'")
 
        # ── Lógica de negocio ──────────────────────────────────────────────
        respuesta = construir_respuesta(payload)
        # ──────────────────────────────────────────────────────────────────
 
        reply_to       = message.properties.get('reply_to')
        correlation_id = message.properties.get('correlation_id')
 
        if not reply_to:
            print(" [!] El mensaje no contiene el campo 'reply_to'. Se descarta.")
            message.ack()
            return
 
        message.channel.basic.publish(
            body=respuesta,
            routing_key=reply_to,
            properties={'correlation_id': correlation_id}
        )
 
        message.ack()
        print(f" [v] Respuesta enviada a '{reply_to}' | corr_id={correlation_id}")
 
    except Exception as e:
        print(f" [!] Error al procesar mensaje: {e}")
        message.nack(requeue=True)
 
 
# ── Bucle principal con reconexión automática ───────────────────────────────
 
def iniciar_worker():
    """Inicia el Worker con reconexión automática ante fallos de conexión."""
    while True:
        try:
            print(" [*] Estableciendo conexión con CloudAMQP...")
            connection = amqpstorm.UriConnection(URL_NUBE)
            channel    = connection.channel()
 
            channel.queue.declare('rpc_queue')
            channel.basic.qos(prefetch_count=1)
            channel.basic.consume(procesar_pedido, queue='rpc_queue')
 
            print(" [*] Worker conectado. Aguardando mensajes en 'rpc_queue'... (CTRL+C para detener)")
            channel.start_consuming()
 
        except amqpstorm.AMQPConnectionError as e:
            print(f" [!] Conexión perdida: {e}. Reintentando en 5 segundos...")
            import time; time.sleep(5)
 
        except KeyboardInterrupt:
            print(" [*] Worker detenido por el operador.")
            break
 
        except Exception as e:
            print(f" [!] Error inesperado: {e}. Reintentando en 5 segundos...")
            import time; time.sleep(5)
 
 
if __name__ == '__main__':
    iniciar_worker()