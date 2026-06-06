import paho.mqtt.client as mqtt
import paho.mqtt.enums as enums

def on_connect(c, userdata, flags, reason, props):
    print('Connected')
    c.subscribe('test/#')

def on_message(c, userdata, msg):
    print(f'Received: {msg.topic} -> {msg.payload.decode()}')

c = mqtt.Client(callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
c.on_connect = on_connect
c.on_message = on_message
c.connect('127.0.0.1', 1883)
c.publish('test/hello', 'hi')
c.loop_forever(timeout=5)