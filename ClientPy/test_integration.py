"""
Integration tests for the MQTT broker.
Each test class starts its own broker instance for isolation.
"""
import subprocess
import time
import os
import json
import unittest
import tempfile
import signal
import sys
import socket
import threading

import paho.mqtt.client as mqtt

_broker_path = os.path.join(os.path.dirname(__file__), "..", "build", "src", "mqtt_broker.exe")
if not os.path.exists(_broker_path):
    _broker_path = _broker_path.replace(".exe", "")
BROKER_BIN = _broker_path
BASE_PORT = 18830

def find_free_port(start):
    for port in range(start, start + 100):
        try:
            s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            s.bind(("127.0.0.1", port))
            s.close()
            return port
        except OSError:
            continue
    raise RuntimeError("No free port found")


class BrokerProcess:
    def __init__(self, port):
        self.port = port
        self.proc = None
        self.config_path = None

    def start(self):
        self.config_path = self._create_config()
        self.proc = subprocess.Popen(
            [BROKER_BIN, self.config_path],
            stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            creationflags=subprocess.CREATE_NEW_PROCESS_GROUP if sys.platform == "win32" else 0
        )
        self._wait_ready()

    def stop(self):
        if self.proc:
            try:
                if sys.platform == "win32":
                    self.proc.send_signal(signal.CTRL_BREAK_EVENT)
                else:
                    self.proc.terminate()
                self.proc.wait(timeout=5)
            except Exception:
                try:
                    self.proc.kill()
                    self.proc.wait(timeout=2)
                except Exception:
                    pass
            self.proc = None
        if self.config_path and os.path.exists(self.config_path):
            try:
                os.remove(self.config_path)
            except OSError:
                pass

    def _create_config(self):
        config = {
            "broker": {
                "host": "127.0.0.1",
                "port": self.port,
                "thread_pool_size": 2,
                "max_connections": 100,
                "max_connections_per_ip": 50,
                "session_expiry_seconds": 86400,
                "keepalive_multiplier": 1.5,
                "max_keepalive": 3600,
                "max_packet_size": 262144,
                "max_topic_depth": 128,
                "max_topic_length": 65536,
                "max_subscriptions_per_client": 100,
                "max_retained_messages": 100,
                "max_write_queue_size": 10000,
                "max_parser_buffer_size": 1048576,
                "max_auth_attempts": 10,
                "auth_ban_seconds": 60,
                "retry_interval_seconds": 5,
                "max_retry_count": 5
            },
            "tls": {"enabled": False},
            "logging": {"level": "error", "file": ""},
            "auth": {"backend": "allow_all", "acl_cache_ttl": 60}
        }
        fd, path = tempfile.mkstemp(suffix=".json", prefix="mqtt_test_")
        with os.fdopen(fd, "w") as f:
            json.dump(config, f)
        return path

    def _wait_ready(self, max_wait=15):
        for _ in range(max_wait * 10):
            try:
                s = socket.create_connection(("127.0.0.1", self.port), timeout=0.5)
                s.close()
                return
            except (ConnectionRefusedError, OSError):
                time.sleep(0.1)
                if self.proc.poll() is not None:
                    out, _ = self.proc.communicate()
                    raise RuntimeError(f"Broker exited prematurely:\n{out.decode(errors='replace')}")
        raise RuntimeError("Broker did not start within timeout")


class MqttClient:
    def __init__(self, client_id=None):
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id
        )
        self.received = []
        self._connected = threading.Event()
        self._conn_rc = None
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, reason, properties):
        self._conn_rc = reason
        self._connected.set()

    def _on_message(self, client, userdata, msg):
        self.received.append(msg)

    def connect(self, host, port, keepalive=30):
        self.client.connect(host, port, keepalive)
        self.client.loop_start()

    def disconnect(self):
        self.client.disconnect()
        self.client.loop_stop()
        self._connected.clear()

    def subscribe(self, topic, qos=0):
        self.client.subscribe(topic, qos)

    def publish(self, topic, payload, qos=0, retain=False):
        info = self.client.publish(topic, payload, qos, retain)
        info.wait_for_publish()
        return info

    def wait_connected(self, timeout=5):
        return self._connected.wait(timeout)

    @property
    def conn_rc(self):
        return self._conn_rc


class BrokerTestCase(unittest.TestCase):
    """Base class that starts/stops a broker per test."""
    broker = None
    _next_port = BASE_PORT

    @classmethod
    def setUpClass(cls):
        cls._next_port += 1
        cls.port = cls._next_port
        cls.broker = BrokerProcess(cls.port)
        cls.broker.start()

    @classmethod
    def tearDownClass(cls):
        if cls.broker:
            cls.broker.stop()
            cls.broker = None

    def make_client(self, client_id=None):
        return MqttClient(client_id=client_id)

    def connect_client(self, client_id=None):
        c = self.make_client(client_id)
        c.connect("127.0.0.1", self.port)
        self.assertTrue(c.wait_connected(), f"Client {client_id} failed to connect")
        return c


class TestConnect(BrokerTestCase):
    def test_clean_session(self):
        c = self.connect_client("con_clean")
        self.assertEqual(c.conn_rc, 0)
        c.disconnect()

    def test_no_clean_session(self):
        c = self.connect_client("con_persist")
        self.assertEqual(c.conn_rc, 0)
        c.disconnect()

    def test_multiple_clients(self):
        clients = [self.connect_client(f"con_multi_{i}") for i in range(5)]
        for c in clients:
            c.disconnect()


class TestPublishSubscribe(BrokerTestCase):
    def test_qos0(self):
        sub = self.connect_client("pub_qos0_sub")
        sub.subscribe("ps/qos0", qos=0)
        time.sleep(0.3)
        pub = self.connect_client("pub_qos0_pub")
        pub.publish("ps/qos0", b"qos0 ok", qos=0)
        for _ in range(10):
            if sub.received:
                break
            time.sleep(0.1)
        msg = sub.received[-1] if sub.received else None
        self.assertIsNotNone(msg)
        self.assertEqual(msg.payload, b"qos0 ok")
        sub.disconnect()
        pub.disconnect()

    def test_qos1(self):
        sub = self.connect_client("pub_qos1_sub")
        sub.subscribe("ps/qos1", qos=1)
        time.sleep(0.3)
        pub = self.connect_client("pub_qos1_pub")
        pub.publish("ps/qos1", b"qos1 ok", qos=1)
        time.sleep(0.5)
        msg = sub.received[-1] if sub.received else None
        self.assertIsNotNone(msg)
        self.assertEqual(msg.payload, b"qos1 ok")
        sub.disconnect()
        pub.disconnect()

    def test_qos2(self):
        sub = self.connect_client("pub_qos2_sub")
        sub.subscribe("ps/qos2", qos=2)
        time.sleep(0.3)
        pub = self.connect_client("pub_qos2_pub")
        pub.publish("ps/qos2", b"qos2 ok", qos=2)
        time.sleep(1.0)
        msg = sub.received[-1] if sub.received else None
        self.assertIsNotNone(msg)
        self.assertEqual(msg.payload, b"qos2 ok")
        sub.disconnect()
        pub.disconnect()

    def test_two_subscribers(self):
        s0 = self.connect_client("ps_2s_s0"); time.sleep(0.2)
        s1 = self.connect_client("ps_2s_s1"); time.sleep(0.2)
        s0.subscribe("ps/two", qos=0); time.sleep(0.3)
        s1.subscribe("ps/two", qos=0); time.sleep(0.3)
        pub = self.connect_client("ps_2s_pub")
        pub.publish("ps/two", b"to two", qos=0)
        time.sleep(1.0)
        self.assertTrue(s0.received, "s0 received nothing")
        self.assertTrue(s1.received, "s1 received nothing")
        self.assertEqual(s0.received[-1].payload, b"to two")
        self.assertEqual(s1.received[-1].payload, b"to two")
        s0.disconnect(); s1.disconnect(); pub.disconnect()

    def test_three_subscribers(self):
        subs = []
        for i in range(3):
            c = self.connect_client(f"ps_3s_s{i}")
            time.sleep(0.2)
            c.subscribe("ps/three", qos=0)
            time.sleep(0.2)
            subs.append(c)
        time.sleep(0.5)
        pub = self.connect_client("ps_3s_pub")
        pub.publish("ps/three", b"to three", qos=0)
        time.sleep(1.0)
        for s in subs:
            self.assertTrue(s.received, f"{s.client._client_id} received nothing")
            self.assertEqual(s.received[-1].payload, b"to three")
            s.disconnect()
        pub.disconnect()


class TestWildcards(BrokerTestCase):
    def test_single_level(self):
        sub = self.connect_client("wc_s_sub")
        sub.subscribe("wc/+/x", qos=0)
        time.sleep(0.3)
        pub = self.connect_client("wc_s_pub")
        pub.publish("wc/foo/x", b"single", qos=0)
        time.sleep(0.3)
        msg = sub.received[-1] if sub.received else None
        self.assertIsNotNone(msg)
        self.assertEqual(msg.topic, "wc/foo/x")
        sub.disconnect()
        pub.disconnect()

    def test_multi_level(self):
        sub = self.connect_client("wc_m_sub")
        sub.subscribe("wc/#", qos=0)
        time.sleep(0.3)
        pub = self.connect_client("wc_m_pub")
        pub.publish("wc/any/deep", b"multi", qos=0)
        time.sleep(0.3)
        msg = sub.received[-1] if sub.received else None
        self.assertIsNotNone(msg)
        self.assertEqual(msg.topic, "wc/any/deep")
        sub.disconnect()
        pub.disconnect()


class TestRetained(BrokerTestCase):
    def test_retain_and_receive(self):
        pub = self.connect_client("rt_pub")
        pub.publish("rt/test", b"sticky", qos=0, retain=True)
        time.sleep(0.2)
        pub.disconnect()
        sub = self.connect_client("rt_sub")
        sub.subscribe("rt/test", qos=0)
        time.sleep(0.3)
        msg = sub.received[-1] if sub.received else None
        self.assertIsNotNone(msg)
        self.assertEqual(msg.payload, b"sticky")
        sub.disconnect()


if __name__ == "__main__":
    if not os.path.exists(BROKER_BIN):
        print(f"Broker binary not found: {BROKER_BIN}")
        sys.exit(1)
    unittest.main(verbosity=2)
