from flask import Flask, request, jsonify
import pika
import json

app = Flask(__name__)
EXCHANGE = 'hechos_exchange'

def enviar_a_rabbitmq(mensaje, routing_key):
    connection = pika.BlockingConnection(pika.ConnectionParameters('localhost'))
    channel = connection.channel()
    channel.exchange_declare(exchange=EXCHANGE, exchange_type='topic')

    channel.basic_publish(
        exchange=EXCHANGE,
        routing_key=routing_key,
        body=json.dumps(mensaje),
        properties=pika.BasicProperties(content_type='application/json')
    )

    print(f"Mensaje enviado con routing_key: {routing_key}")
    connection.close()

@app.route('/enviar-evento', methods=['POST'])
def recibir_evento():
    try:
        mensaje = request.get_json()
        if not mensaje:
            return jsonify({"error": "No se recibió un JSON válido"}), 400

        # Verificar campos requeridos generales
        campos = [
            "direccion", "interseccion", "numero_casa", "latitud", "longitud",
            "tipo_lugar", "sector_punto_referencia", "fecha_hecho", "hora_aproximada_hecho",
            "enlace_fuente", "transcripción_de_video", "transcripción_de_audio"
        ]
        faltantes = [campo for campo in campos if campo not in mensaje]
        if faltantes:
            return jsonify({"error": "Faltan campos", "faltantes": faltantes}), 400

        # Detectar tipo de dato automáticamente y enviar con routing_key correspondiente
        enviado = False

        if mensaje.get("transcripción_de_audio", "").strip():
            enviar_a_rabbitmq(mensaje, "hecho.delictivo.audio")
            enviado = True

        if mensaje.get("transcripción_de_video", "").strip():
            enviar_a_rabbitmq(mensaje, "hecho.delictivo.texto")
            enviado = True

        if mensaje.get("latitud") and mensaje.get("longitud"):
            enviar_a_rabbitmq(mensaje, "hecho.delictivo.ubicacion")
            enviado = True

        if not enviado:
            return jsonify({"error": "No se encontró ningún tipo de dato válido (audio, texto o ubicación)"}), 400

        return jsonify({"mensaje": "✅ Mensaje(s) enviado(s) a RabbitMQ"}), 200

    except Exception as e:
        return jsonify({"error": str(e)}), 500

if __name__ == '__main__':
    app.run(debug=True)
