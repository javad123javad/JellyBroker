import paho.mqtt.client as mqtt
import time

received = []

def on_connect(client, userdata, flags, rc, props=None):
    print(f"Connected (rc={rc})")
    client.subscribe("test/#")

def on_message(client, userdata, msg):
    print(f"Got: {msg.topic} -> {msg.payload.decode()}")
    received.append(msg)

def on_subscribe(client, userdata, mid, granted_qos, props=None):
    print(f"Subscribed, publishing...")
    info = client.publish("test/hello", "hi from python")
    print(f"Publish rc={info.rc}, mid={info.mid}")

try:
    c = mqtt.Client(mqtt.CallbackAPIVersion.VERSION2)
except AttributeError:
    c = mqtt.Client()

c.on_connect = on_connect
c.on_subscribe = on_subscribe
c.on_message = on_message
c.connect("127.0.0.1", 1883, keepalive=60)
c.loop_start()
time.sleep(3)
c.loop_stop()
print(f"Done. Received {len(received)} messages")
for m in received:
    print(f"  {m.topic}: {m.payload.decode()}")
