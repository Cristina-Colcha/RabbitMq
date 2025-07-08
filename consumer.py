import pika
import json

# Función para procesar los mensajes
def callback(ch, method, properties, body):
    data = json.loads(body)
    print("📥 Mensaje recibido:")
    print(json.dumps(data, indent=2, ensure_ascii=False))  # Imprime bonito en consola

# Conexión a RabbitMQ
connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
channel = connection.channel()

# Asegurar que la cola exista
channel.queue_declare(queue='hechos_delictivos')

# Escuchar la cola
channel.basic_consume(queue='hechos_delictivos', on_message_callback=callback, auto_ack=True)

print("⏳ Esperando mensajes. Presiona Ctrl+C para salir.")
channel.start_consuming()
