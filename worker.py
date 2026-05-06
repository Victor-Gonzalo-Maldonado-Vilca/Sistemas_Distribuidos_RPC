import amqpstorm
import threading
import http.server
import socketserver
import os
import sys
 
sys.stdout.reconfigure(line_buffering=True)
 
# ── URL de CloudAMQP ────────────────────────────────────────────────────────
URL_NUBE = "amqps://vdhlnbov:ogNW_b6xOVvycixvXj2uyAJOpWgCbmkx@chameleon.lmq.cloudamqp.com/vdhlnbov"
 
 
# ── Servidor HTTP de mantenimiento ──────────────────────────────────────────
# Render requiere que el proceso escuche en un puerto para no matarlo.
def run_dummy_server():
    PORT = int(os.environ.get("PORT", 10000))
    socketserver.TCPServer.allow_reuse_address = True
 
    class SilentHandler(http.server.SimpleHTTPRequestHandler):
        """Handler que no imprime logs de cada request para no ensuciar la consola."""
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
 
def procesar_pedido(message):
    """Procesa un mensaje de la rpc_queue y envía la respuesta al cliente."""
    try:
        payload = message.body
        if isinstance(payload, bytes):
            payload = payload.decode('utf-8')
 
        print(f" [x] Recibido: '{payload}'")
 
        # ── Lógica de negocio ──────────────────────────────────────────────
        # Modifica esta sección para implementar tu propia lógica.
        respuesta = f"RESULTADO CLOUD: {payload.upper()}"
        # ──────────────────────────────────────────────────────────────────
 
        reply_to = message.properties.get('reply_to')
        correlation_id = message.properties.get('correlation_id')
 
        if not reply_to:
            print(" [!] El mensaje no tiene 'reply_to'. Se descarta.")
            message.ack()
            return
 
        # Publicar la respuesta en la cola de respuestas del cliente
        message.channel.basic.publish(
            body=respuesta,
            routing_key=reply_to,
            properties={
                'correlation_id': correlation_id
            }
        )
 
        message.ack()
        print(f" [v] Respuesta enviada: '{respuesta}'")
 
    except Exception as e:
        print(f" [!] Error procesando mensaje: {e}")
        # No hacemos ack para que el mensaje vuelva a la cola
        message.nack(requeue=True)
 
 
# ── Bucle principal con reconexión automática ───────────────────────────────
 
def iniciar_worker():
    """Inicia el worker con reconexión automática ante fallos."""
    while True:
        try:
            print(" [*] Conectando a CloudAMQP...")
            connection = amqpstorm.UriConnection(URL_NUBE)
            channel = connection.channel()
 
            # Declarar la cola por si no existe aún
            channel.queue.declare('rpc_queue')
 
            # QOS: procesar de a 1 mensaje para no saturar
            channel.basic.qos(prefetch_count=1)
 
            # Registrar el consumidor
            channel.basic.consume(procesar_pedido, queue='rpc_queue')
 
            print(" [*] CONECTADO. Esperando mensajes en 'rpc_queue'... (CTRL+C para salir)")
            channel.start_consuming()
 
        except amqpstorm.AMQPConnectionError as e:
            print(f" [!] Conexión perdida: {e}. Reintentando en 5 segundos...")
            import time
            time.sleep(5)
 
        except KeyboardInterrupt:
            print(" [*] Worker detenido manualmente.")
            break
 
        except Exception as e:
            print(f" [!] Error inesperado: {e}. Reintentando en 5 segundos...")
            import time
            time.sleep(5)
 
 
if __name__ == '__main__':
    iniciar_worker()