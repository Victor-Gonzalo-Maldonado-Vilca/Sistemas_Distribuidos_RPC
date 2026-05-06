import amqpstorm
import threading
import http.server
import socketserver
import os

def run_dummy_server():
    """Este servidor solo sirve para que Render no apague el proceso."""
    PORT = int(os.environ.get("PORT", 10000))
    handler = http.server.SimpleHTTPRequestHandler
    with socketserver.TCPServer(("", PORT), handler) as httpd:
        print(f" [!] Servidor de mantenimiento activo en puerto {PORT}")
        httpd.serve_forever()

threading.Thread(target=run_dummy_server, daemon=True).start()

def procesar_pedido(message):
    payload = message.body
    if isinstance(payload, bytes):
        payload = payload.decode()
    
    print(f" [x] Recibido de la Nube: {payload}")
    respuesta = f"RESULTADO CLOUD: {payload.upper()}"

    message.channel.basic.publish(
        body=respuesta,
        routing_key=message.properties['reply_to'],
        properties={'correlation_id': message.properties['correlation_id']}
    )
    message.ack()
    print(f" [v] Respuesta enviada: {respuesta}")

URL_NUBE = "amqps://vdhlnbov:ogNW_b6xOVvycixvXj2uyAJOpWgCbmkx@chameleon.lmq.cloudamqp.com/vdhlnbov"

try:
    connection = amqpstorm.UriConnection(URL_NUBE)
    channel = connection.channel()
    channel.queue.declare('rpc_queue')
    channel.basic.consume(procesar_pedido, queue='rpc_queue')

    print(" [*] Worker conectado a CloudAMQP. Esperando datos...")
    channel.start_consuming()

except Exception as e:
    print(f" [!] Error en el Worker: {e}")