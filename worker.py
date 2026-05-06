import amqpstorm

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

# USANDO TU URL DE CLOUDAMQP
URL_NUBE = "amqps://vdhlnbov:ogNW_b6xOVvycixvXj2uyAJOpWgCbmkx@chameleon.lmq.cloudamqp.com/vdhlnbov"
connection = amqpstorm.UriConnection(URL_NUBE)
channel = connection.channel()
channel.queue.declare('rpc_queue')
channel.basic.consume(procesar_pedido, queue='rpc_queue')

print(" [*] Worker conectado a CloudAMQP. Esperando datos...")
channel.start_consuming()