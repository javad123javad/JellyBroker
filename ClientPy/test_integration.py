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
import ssl
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

def generate_test_cert(cert_dir):
    key_path = os.path.join(cert_dir, "server.key")
    cert_path = os.path.join(cert_dir, "server.crt")
    if os.path.exists(key_path) and os.path.exists(cert_path):
        return cert_path, key_path

    openssl = "openssl"
    try:
        subprocess.run([openssl, "version"], capture_output=True, check=True)
    except (FileNotFoundError, subprocess.CalledProcessError):
        return None, None

    subprocess.run(
        [openssl, "req", "-x509", "-newkey", "rsa:2048",
         "-keyout", key_path, "-out", cert_path,
         "-days", "3650", "-nodes",
         "-subj", "/CN=localhost"],
        capture_output=True, check=True)
    return cert_path, key_path


class BrokerProcess:
    def __init__(self, port, use_tls=False, admin_tls=False, admin_port=None,
                 auth_backend=None, pg_conn_str=None):
        self.port = port
        self.use_tls = use_tls
        self.admin_tls = admin_tls
        self.admin_port = admin_port or (port + 1)
        self.auth_backend = auth_backend
        self.pg_conn_str = pg_conn_str
        self.proc = None
        self.config_path = None
        self.cert_dir = None

    def start(self):
        self.cert_dir = tempfile.mkdtemp(prefix="mqtt_certs_")
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
        if self.cert_dir and os.path.exists(self.cert_dir):
            try:
                for f in os.listdir(self.cert_dir):
                    os.remove(os.path.join(self.cert_dir, f))
                os.rmdir(self.cert_dir)
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
            "auth": {"backend": self.auth_backend or "allow_all", "acl_cache_ttl": 0},
            "mdns": {"enabled": False},
            "admin": {"enabled": False}
        }

        if self.use_tls:
            cert_path, key_path = generate_test_cert(self.cert_dir)
            if cert_path:
                config["tls"] = {
                    "enabled": True,
                    "cert_file": cert_path,
                    "key_file": key_path,
                    "ca_file": ""
                }

        if self.pg_conn_str:
            config["auth"]["postgres"] = {
                "connection_string": self.pg_conn_str,
                "pool_size": 2
            }

        config["admin"] = {
            "enabled": self.admin_tls or False,
            "port": self.admin_port,
            "tls_enabled": self.admin_tls or False
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
    def __init__(self, client_id=None, use_tls=False):
        self.client = mqtt.Client(
            callback_api_version=mqtt.CallbackAPIVersion.VERSION2,
            client_id=client_id
        )
        self.received = []
        self._connected = threading.Event()
        self._conn_rc = None
        self._use_tls = use_tls
        self.client.on_connect = self._on_connect
        self.client.on_message = self._on_message

    def _on_connect(self, client, userdata, flags, reason, properties):
        self._conn_rc = reason
        self._connected.set()

    def _on_message(self, client, userdata, msg):
        self.received.append(msg)

    def connect(self, host, port, keepalive=30):
        if self._use_tls:
            self.client.tls_set(cert_reqs=ssl.CERT_NONE)
            self.client.tls_insecure_set(True)
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


class BrokerTlsTestCase(unittest.TestCase):
    broker = None
    _next_port = BASE_PORT + 100

    @classmethod
    def setUpClass(cls):
        cert = generate_test_cert(tempfile.mkdtemp(prefix="mqtt_certs_"))
        if cert[0] is None:
            raise unittest.SkipTest("openssl not available, skipping TLS tests")
        cls._next_port += 1
        cls.port = cls._next_port
        cls.admin_port = cls.port + 1
        cls.broker = BrokerProcess(cls.port, use_tls=True, admin_tls=True,
                                   admin_port=cls.admin_port)
        cls.broker.start()

    @classmethod
    def tearDownClass(cls):
        if cls.broker:
            cls.broker.stop()
            cls.broker = None

    def make_client(self, client_id=None):
        return MqttClient(client_id=client_id, use_tls=True)

    def connect_client(self, client_id=None):
        c = self.make_client(client_id)
        c.connect("127.0.0.1", self.port)
        self.assertTrue(c.wait_connected(), f"Client {client_id} failed to connect over TLS")
        return c


class TestTls(BrokerTlsTestCase):
    def test_mqtt_connect_tls(self):
        c = self.connect_client("tls_con")
        self.assertEqual(c.conn_rc, 0)
        c.disconnect()

    def test_mqtt_pub_sub_tls(self):
        sub = self.connect_client("tls_sub")
        sub.subscribe("tls/test", qos=1)
        time.sleep(0.3)
        pub = self.connect_client("tls_pub")
        pub.publish("tls/test", b"tls works", qos=1)
        time.sleep(0.5)
        msg = sub.received[-1] if sub.received else None
        self.assertIsNotNone(msg)
        self.assertEqual(msg.payload, b"tls works")
        sub.disconnect()
        pub.disconnect()

    def test_admin_tls_endpoint(self):
        ctx = ssl.create_default_context(cafile=None)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        raw = socket.create_connection(("127.0.0.1", self.admin_port), timeout=5)
        with ctx.wrap_socket(raw, server_hostname="localhost") as tls:
            tls.sendall(b"STATS\n")
            resp = tls.recv(4096).decode()
        self.assertIn("active_connections", resp)
        self.assertIn("published", resp)

    def test_admin_tls_clients(self):
        ctx = ssl.create_default_context(cafile=None)
        ctx.check_hostname = False
        ctx.verify_mode = ssl.CERT_NONE
        raw = socket.create_connection(("127.0.0.1", self.admin_port), timeout=5)
        with ctx.wrap_socket(raw, server_hostname="localhost") as tls:
            tls.sendall(b"CLIENTS\n")
            resp = tls.recv(4096).decode()
        self.assertEqual(resp.strip(), "[]")


PG_CONN_STR = os.environ.get("PG_CONN_STR", "")


class PostgresAuthTestCase(unittest.TestCase):
    broker = None
    _next_port = BASE_PORT + 200

    @classmethod
    def setUpClass(cls):
        if not PG_CONN_STR:
            raise unittest.SkipTest("PG_CONN_STR not set, skipping PostgreSQL auth tests")
        cls._next_port += 1
        cls.port = cls._next_port
        cls.broker = BrokerProcess(cls.port, auth_backend="postgres",
                                   pg_conn_str=PG_CONN_STR)
        cls.broker.start()

    @classmethod
    def tearDownClass(cls):
        if cls.broker:
            cls.broker.stop()
            cls.broker = None

    def make_client(self, client_id=None):
        return MqttClient(client_id=client_id)

    def connect_client(self, client_id=None, username=None, password=None):
        c = self.make_client(client_id)
        if username is not None:
            c.client.username_pw_set(username, password)
        c.connect("127.0.0.1", self.port)
        self.assertTrue(c.wait_connected(),
                        f"Client {client_id} failed to connect with auth")
        return c

    def test_pg_auth_connect(self):
        """Connect with valid PostgreSQL credentials succeeds."""
        c = self.connect_client("pg_test_user", "pg_test_user", "pg_test_pass")
        self.assertEqual(c.conn_rc, 0)
        c.disconnect()

    def test_pg_auth_bad_password(self):
        """Connect with bad password is rejected."""
        c = self.make_client("pg_test_user")
        c.client.username_pw_set("pg_test_user", "wrong_password")
        c.connect("127.0.0.1", self.port)
        c.wait_connected(timeout=3)
        c.disconnect()
        self.assertNotEqual(c.conn_rc, 0, "Bad password should return non-zero CONNACK")

    def test_pg_auth_publish_allowed(self):
        """Publish to an allowed topic succeeds."""
        c = self.connect_client("pg_test_user", "pg_test_user", "pg_test_pass")
        c.subscribe("pg/#", qos=0)
        time.sleep(0.3)
        c.publish("pg/test", b"pg works", qos=0)
        time.sleep(0.5)
        self.assertTrue(c.received, "Should receive message on allowed topic")
        self.assertEqual(c.received[-1].payload, b"pg works")
        c.disconnect()

    def test_pg_auth_publish_forbidden(self):
        """Publish to a topic not matching ACL is silently dropped."""
        sub = self.connect_client("pg_sub", username="pg_test_user", password="pg_test_pass")
        sub.subscribe("test/#", qos=0)
        time.sleep(0.3)
        pub = self.connect_client("pg_test_user", "pg_test_user", "pg_test_pass")
        # ACL has test/+ with READ access only — WRITE should be denied
        pub.publish("test/forbidden", b"should be dropped", qos=0)
        time.sleep(0.5)
        # The message should NOT be delivered to subscribers
        self.assertFalse(sub.received,
                         "ACL-denied publish should not be delivered to subscribers")
        sub.disconnect()
        pub.disconnect()


if __name__ == "__main__":
    if not os.path.exists(BROKER_BIN):
        print(f"Broker binary not found: {BROKER_BIN}")
        sys.exit(1)
    unittest.main(verbosity=2)
