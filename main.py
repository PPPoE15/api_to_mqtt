import requests
from paho.mqtt import client as mqtt_client
import random
import time

''' MQTT-broker settings '''
broker = 'test.mosquitto.org'
port = 8885
client_id = f'client - {random.randint(0, 1000)}'
username = 'wo'
password = 'writeonly'
cert_path = 'certs/mosquitto.org.crt'

''' API settings '''
idList = ['S50', 'S107', 'S60']
api_url = "https://api.data.gov.sg/v1/environment/air-temperature"


def connect_mqtt():
    def on_connect(client, userdata, flags, rc):
        if rc == 0:
            print("Connected to MQTT Broker!")
        else:
            print("Failed to connect, return code %dn", rc)

    client = mqtt_client.Client(client_id)
    client.tls_set(cert_path)
    client.username_pw_set(username, password)
    client.on_connect = on_connect
    client.connect(broker, port)
    return client


def publish(client, topic, message):
    result = client.publish(topic, message)
    status = result[0]
    if status == 0:
        print(f"Send {message} to topic {topic}")
    else:
        print(f"Failed to send message to topic {topic}")


def api_loop(client):
    while True:
        time.sleep(1)
        api_json = requests.get(api_url).json()  # connecting to api and getting JSON
        api_info = api_json['api_info']['status']  # getting api-status
        publish(client, '/api/status', api_info)

        readings = api_json['items'][0]['readings']  # getting temp readings
        for id in range(len(readings)):
            if readings[id]['station_id'] in idList:
                publish(client, f"/api/temperature/{readings[id]['station_id']}", readings[id]['value'])


def run():
    client = connect_mqtt()
    client.loop_start()
    api_loop(client)


if __name__ == '__main__':
    run()
