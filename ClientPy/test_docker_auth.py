"""
End-to-end Docker Compose PostgreSQL auth test.
Usage:
    python ClientPy/test_docker_auth.py [--build] [--no-cleanup]

Starts broker + PostgreSQL via docker-compose, seeds test users,
runs auth tests, then tears down.
"""
import subprocess
import time
import os
import sys
import socket
import ssl
import argparse
import hashlib
import threading

import paho.mqtt.client as mqtt

BASE_DIR = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
TEST_USER = "docker_test_user"
TEST_PASS = "docker_test_pass"
SALT = "dockersalt"
TOPIC_ALLOW = "pg/hello"
TOPIC_DENY = "test/forbidden"


def compute_hash(password, salt):
    return hashlib.sha256((salt + password).encode()).hexdigest()


MqttClient = None  # forward ref for struct-like usage


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

    def connect(self, host, port, username=None, password=None, keepalive=30):
        if username is not None:
            self.client.username_pw_set(username, password)
        self.client.connect(host, port, keepalive)
        self.client.loop_start()

    def disconnect(self):
        self.client.disconnect()
        self.client.loop_stop()
        self._connected.clear()

    def subscribe(self, topic, qos=0):
        self.client.subscribe(topic, qos)

    def publish(self, topic, payload, qos=0):
        info = self.client.publish(topic, payload, qos)
        info.wait_for_publish()
        return info

    def wait_connected(self, timeout=5):
        return self._connected.wait(timeout)


class DockerComposeTest:
    def __init__(self, compose_dir, build=False, no_cleanup=False):
        self.compose_dir = compose_dir
        self.build = build
        self.no_cleanup = no_cleanup
        self.host = "127.0.0.1"
        self.port = 1883

    def _run_compose(self, *args):
        cmd = ["docker", "compose"]
        if self.compose_dir:
            cmd += ["-f", os.path.join(self.compose_dir, "docker-compose.test.yml")]
        cmd += list(args)
        return subprocess.run(cmd, capture_output=True, text=True)

    def start(self):
        print("=== Starting Docker Compose stack ===")
        args = ["up", "-d", "--wait"]
        if self.build:
            args.append("--build")
        r = self._run_compose(*args)
        if r.returncode != 0:
            print(r.stdout)
            print(r.stderr)
            raise RuntimeError("docker compose up failed")
        print("Stack is healthy")

    def stop(self):
        if self.no_cleanup:
            print("Skipping cleanup (--no-cleanup)")
            return
        print("=== Tearing down ===")
        self._run_compose("down", "-v")

    def seed_user(self):
        print("=== Seeding test user ===")
        pw_hash = compute_hash(TEST_PASS, SALT)
        sql = (
            f"INSERT INTO clients (client_id, password_hash, salt, enabled) "
            f"VALUES ('{TEST_USER}', '{pw_hash}', '{SALT}', true) "
            f"ON CONFLICT (client_id) DO NOTHING;\n"
            f"INSERT INTO acls (username, topic_filter, access) "
            f"VALUES ('{TEST_USER}', 'pg/#', 2) "
            f"ON CONFLICT DO NOTHING;\n"
            f"INSERT INTO acls (username, topic_filter, access) "
            f"VALUES ('{TEST_USER}', 'test/+', 1) "
            f"ON CONFLICT DO NOTHING;\n"
        )
        r = subprocess.run(
            ["docker", "compose", "-f",
             os.path.join(self.compose_dir, "docker-compose.test.yml"),
             "exec", "-T", "postgres", "psql", "-U", "mqtt", "-d", "mqtt"],
            input=sql, capture_output=True, text=True
        )
        if r.returncode != 0:
            print(r.stdout, r.stderr)
            raise RuntimeError("Failed to seed test user")
        print("Test user seeded")

    def cleanup_user(self):
        if self.no_cleanup:
            return
        sql = f"DELETE FROM clients WHERE client_id = '{TEST_USER}';\n"
        sql += f"DELETE FROM acls WHERE username = '{TEST_USER}';\n"
        subprocess.run(
            ["docker", "compose", "-f",
             os.path.join(self.compose_dir, "docker-compose.test.yml"),
             "exec", "-T", "postgres", "psql", "-U", "mqtt", "-d", "mqtt"],
            input=sql, capture_output=True, text=True
        )

    def wait_broker(self, timeout=30):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            try:
                s = socket.create_connection((self.host, self.port), timeout=2)
                s.close()
                return
            except (ConnectionRefusedError, OSError):
                time.sleep(1)
        raise RuntimeError("Broker did not become ready")


def run_tests(dct):
    passed = 0
    failed = 0

    def test(name, fn):
        nonlocal passed, failed
        try:
            fn()
            print(f"  PASS  {name}")
            passed += 1
        except Exception as e:
            print(f"  FAIL  {name}: {e}")
            failed += 1

    print("\n=== Running tests ===")

    def test_connect_success():
        c = MqttClient(TEST_USER)
        c.connect(dct.host, dct.port, username=TEST_USER, password=TEST_PASS)
        ok = c.wait_connected(timeout=5)
        c.disconnect()
        if not ok:
            raise AssertionError("Did not connect")
        if c._conn_rc != 0:
            raise AssertionError(f"CONNACK={c._conn_rc}, expected 0")

    def test_bad_password():
        c = MqttClient(TEST_USER)
        c.connect(dct.host, dct.port, username=TEST_USER, password="wrong")
        c.wait_connected(timeout=3)
        c.disconnect()
        if c._conn_rc == 0:
            raise AssertionError("CONNACK=0, expected non-zero for bad password")

    def test_publish_allowed():
        c = MqttClient(TEST_USER)
        c.connect(dct.host, dct.port, username=TEST_USER, password=TEST_PASS)
        c.wait_connected(timeout=5)
        c.subscribe("pg/#", qos=0)
        time.sleep(0.3)
        c.publish(TOPIC_ALLOW, b"docker pg works", qos=0)
        time.sleep(0.5)
        if not c.received:
            raise AssertionError("No message received on allowed topic")
        if c.received[-1].payload != b"docker pg works":
            raise AssertionError(f"Unexpected payload: {c.received[-1].payload}")
        c.disconnect()

    def test_publish_denied():
        c = MqttClient(TEST_USER)
        c.connect(dct.host, dct.port, username=TEST_USER, password=TEST_PASS)
        c.wait_connected(timeout=5)
        c.subscribe("test/#", qos=0)
        time.sleep(0.3)
        c.publish(TOPIC_DENY, b"should be dropped", qos=0)
        time.sleep(0.5)
        if c.received:
            raise AssertionError("Message was delivered despite ACL denial")
        c.disconnect()

    test("Connect with valid credentials", test_connect_success)
    test("Connect with bad password rejected", test_bad_password)
    test("Publish to allowed topic", test_publish_allowed)
    test("Publish to denied topic silently dropped", test_publish_denied)

    print(f"\n=== Results: {passed} passed, {failed} failed ===")
    return failed == 0


def main():
    parser = argparse.ArgumentParser(description="Docker Compose PG auth test")
    parser.add_argument("--build", action="store_true", help="Rebuild Docker image")
    parser.add_argument("--no-cleanup", action="store_true",
                        help="Leave containers running after test")
    args = parser.parse_args()

    dct = DockerComposeTest(BASE_DIR, build=args.build, no_cleanup=args.no_cleanup)

    try:
        dct.start()
        dct.wait_broker()
        dct.seed_user()
        success = run_tests(dct)
    finally:
        dct.cleanup_user()
        dct.stop()

    sys.exit(0 if success else 1)


if __name__ == "__main__":
    main()
