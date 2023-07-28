from flask import Flask, jsonify
from flask_cors import CORS
from flask_pymongo import PyMongo
import pymongo
from datetime import datetime
import paho.mqtt.client as mqtt
import json

app = Flask(__name__)
app.config['MONGO_URI'] = 'mongodb+srv://saochoa1:bgiT6rF3J4EzjVDa@clusterdashboardic.erg0gfr.mongodb.net/ic_dashboard'
mongo = PyMongo(app)
live_data = []

# Enable CORS
CORS(app)

# Setting up the username and password for the MQTT broker
mqtt_broker = "nam1.cloud.thethings.network"
mqtt_port = 1883
mqtt_username = "sensores-espe@ttn"
mqtt_password = "NNSXS.3JABFSPM25WQAW4FCASLBX4PICR75GMDDJNVXXA.3Y7ZIHJKDS7QUEG67WSFJF55DD7YAB3A5CCOZHW5RIX6ZEWBJLJQ"  # "NNSXS.ZL4ZNNGP77E6YB73I7ZARXEUMSB2RESQWXAWEUA.HJDVR7SHXTEYCCQIWQDSPHHHNZWABA3NNNOJNHHFS3KJQ35UIE7Q"


# Setting up callbacks
def on_connect(client, user_data, flags, rc):
    print("Connected to broker MQTT with result code " + str(rc) + " using flags " + str(flags))
    client.subscribe("v3/sensores-espe@ttn/devices/eui-ics23/up")


def on_message(client, user_data, msg):
    payload = msg.payload.decode("utf-8")
    payload_data = json.loads(payload)
    received_at_str = payload_data["received_at"]
    insert_date = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    sensor_data = payload_data["uplink_message"]["decoded_payload"]
    collection_data = {'payloadDecoded': sensor_data, 'received_at': received_at_str, 'inserted_at': insert_date}
    mongo.db.sensor_data.insert_one(collection_data)
    live_data.append(collection_data)
    print(collection_data)


# Setting up the MQTT client
mqtt_client = mqtt.Client()
mqtt_client.username_pw_set(mqtt_username, mqtt_password)
mqtt_client.on_connect = on_connect
mqtt_client.on_message = on_message


# Connecting to the MQTT broker on separate thread
def connect_mqtt():
    mqtt_client.connect(mqtt_broker, mqtt_port, 60)
    mqtt_client.loop_start()


@app.route('/connect')
def connect():
    connect_mqtt()
    return "Connected to MQTT broker"


@app.route('/api/v1/sensors/live-data/', methods=['GET'])
def get_live_data():
    return jsonify(live_data)


@app.route('/api/v1/sensors/live-data/temperature', methods=['GET'])
def get_live_data_by_temperature():
    response = []
    for data in live_data:
        response.append({
            '_id': str(data['_id']),
            'temperature_1': data['payloadDecoded']['temperature_1'],
            'inserted_at': data['inserted_at']
        })
    return jsonify(response)


@app.route('/api/v1/sensors/live-data/relative-humidity', methods=['GET'])
def get_live_data_by_relative_humidity():
    response = []
    for data in live_data:
        response.append({
            '_id': str(data['_id']),
            'relative_humidity_2': data['payloadDecoded']['relative_humidity_2'],
            'inserted_at': data['inserted_at']
        })
    return jsonify(response)


@app.route('/api/v1/sensors/live-data/luminosity', methods=['GET'])
def get_live_data_by_luminosity():
    response = []
    for data in live_data:
        response.append({
            '_id': str(data['_id']),
            'luminosity_3': data['payloadDecoded']['luminosity_3'],
            'inserted_at': data['inserted_at']
        })
    return jsonify(response)


@app.route('/api/v1/sensors/live-data/barometric-pressure', methods=['GET'])
def get_live_data_by_barometric_pressure():
    response = []
    for data in live_data:
        response.append({
            '_id': str(data['_id']),
            'barometric_pressure_4': data['payloadDecoded']['barometric_pressure_4'],
            'inserted_at': data['inserted_at']
        })
    return jsonify(response)


@app.route('/api/v1/sensors/storage-data/temperature', methods=['GET'])
def get_storage_data_by_temperature():
    temperatures = []
    cursor = mongo.db.sensor_data.find({}, {"payloadDecoded.temperature_1": 1, "inserted_at": 1})
    for doc in cursor:
        temperatures.append({
            '_id': str(doc['_id']),
            'temperature_1': doc['payloadDecoded']['temperature_1'],
            'inserted_at': doc['inserted_at']
        })

    return jsonify(temperatures)


@app.route('/api/v1/sensors/storage-data/relative-humidity', methods=['GET'])
def get_storage_data_by_relative_humidity():
    r_humidity = []
    cursor_hum = mongo.db.sensor_data.find({}, {"payloadDecoded.relative_humidity_2": 1, "inserted_at": 1})
    for doc in cursor_hum:
        r_humidity.append({
            '_id': str(doc['_id']),
            'relative_humidity_2': doc['payloadDecoded']['relative_humidity_2'],
            'inserted_at': doc['inserted_at']
        })

    return jsonify(r_humidity)


@app.route('/api/v1/sensors/storage-data/luminosity', methods=['GET'])
def get_storage_data_by_luminosity():
    luminosities = []
    cursor_lum = mongo.db.sensor_data.find({}, {"payloadDecoded.luminosity_3": 1, "inserted_at": 1})
    for doc in cursor_lum:
        luminosities.append({
            '_id': str(doc['_id']),
            'luminosity_3': doc['payloadDecoded']['luminosity_3'],
            'inserted_at': doc['inserted_at']
        })
    return jsonify(luminosities)


@app.route('/api/v1/sensors/storage-data/barometric-pressure', methods=['GET'])
def get_storage_data_by_barometric_pressure():
    barometric_pressures = []
    cursor = mongo.db.sensor_data.find({}, {"payloadDecoded.barometric_pressure_4": 1, "inserted_at": 1})
    for doc in cursor:
        barometric_pressures.append({
            '_id': str(doc['_id']),
            'barometric_pressure_4': doc['payloadDecoded']['barometric_pressure_4'],
            'inserted_at': doc['inserted_at']
        })
    return jsonify(barometric_pressures)


if __name__ == '__main__':
    app.run(host="0.0.0.0", port=5000, debug=True)
