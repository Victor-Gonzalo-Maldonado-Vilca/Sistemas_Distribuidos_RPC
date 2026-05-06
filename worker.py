import amqpstorm
import threading
import http.server
import socketserver
import os
import sys

sys.stdout.reconfigure(line_buffering=True)

def run_dummy_server():
    """Servidor para que Render detecte un puerto abierto y no mate el proceso."""
    PORT = int(os.environ.get("PORT", 10000))
    handler = http.server.SimpleHTTPRequestHandler
    # Permitimos reutilizar la dirección para evitar errores de 'Address already in use'
    socketserver.TCPServer.allow_reuse_address = True
    with socketserver.TCPServer(("", PORT), handler) as httpd:
        print(f" [!] Servidor de mantenimiento Render activo en puerto {PORT}")
        httpd.serve_forever()

# Lanzamos el servidor en un hilo separado (segundo plano)
threading.Thread(target=run_dummy_server, daemon=True).start()

def procesar_pedido(message):
    """Función que procesa los mensajes de la rpc_queue."""
    try:
        payload = message.body
        if isinstance(payload, bytes):
            payload = payload.decode()
        
        print(f" [x] Recibido de la Nube: {payload}")
        
        # Lógica de negocio: Convertir a mayúsculas
        respuesta = f"RESULTADO CLOUD: {payload.upper()}"

        # Enviamos la respuesta a la cola que nos indique el cliente
        message.channel.basic.publish(
            body=respuesta,
            routing_key=message.properties['reply_to'],
            properties={
                'correlation_id': message.properties['correlation_id']
            }
        )
        # Confirmamos que procesamos el mensaje
        message.ack()
        print(f" [v] Respuesta enviada con éxito: {respuesta}")
        
    except Exception as e:
        print(f" [!] Error procesando mensaje: {e}")

# URL DE CLOUDAMQP
URL_NUBE = "amqps://vdhlnbov:ogNW_b6xOVvycixvXj2uyAJOpWgCbmkx@chameleon.lmq.cloudamqp.com/vdhlnbov"

print(" [*] Intentando conectar a CloudAMQP...")

try:
    # Conexión estándar sin parámetros extraños para evitar errores de argumentos
    connection = amqpstorm.UriConnection(URL_NUBE, heartbeat=60)
    channel = connection.channel()
    
    # Declaramos la cola por si no existe
    channel.queue.declare('rpc_queue')
    
    # QOS: Indica que el worker solo tome 1 mensaje a la vez (evita saturación)
    channel.basic.qos(prefetch_count=1)
    
    # Configuramos el consumidor
    channel.basic.consume(procesar_pedido, queue='rpc_queue')

    print(" [*] CONECTADO EXITOSAMENTE. Esperando datos en 'rpc_queue'...")
    channel.start_consuming()

except Exception as e:
    print(f" [!] Error fatal en el Worker: {e}")
    sys.exit(1)