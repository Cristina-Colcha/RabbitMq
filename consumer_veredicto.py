import pika
import json

def callback(ch, method, properties, body):
    data = json.loads(body)
    print(" [VEREDICTO] Recibido:")
    print(json.dumps(data, indent=2, ensure_ascii=False))

connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

exchange = 'hechos_exchange'
queue = 'cola_veredicto'

channel.exchange_declare(exchange=exchange, exchange_type='topic')
channel.queue_declare(queue=queue)
channel.queue_bind(exchange=exchange, queue=queue, routing_key='hecho.#')

channel.basic_consume(queue=queue, on_message_callback=callback, auto_ack=True)
print("🧠 Esperando todos los eventos para análisis final...")
channel.start_consuming()
