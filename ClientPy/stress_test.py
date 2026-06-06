"""
MQTT Broker Stress Test
Usage: python stress_test.py [host] [port] [options]

Tests: connection storm, publish throughput, subscribe delivery accuracy
"""

import paho.mqtt.client as mqtt
import time
import threading
import sys
import argparse
from collections import defaultdict

RESULTS = {}
lock = threading.Lock()

def report(name, value, unit=""):
    with lock:
        RESULTS[name] = (value, unit)

def client_id(prefix, n):
    return f"{prefix}_{n}_{int(time.time()*1000000)%1000000}"

def connection_storm(host, port, count):
    """Connect `count` clients sequentially, measure time."""
    print(f"\n=== Connection Storm: {count} clients ===")
    start = time.time()
    connected = 0
    errors = 0

    def on_connect(c, ud, flags, rc, props=None):
        nonlocal connected
        if rc == 0:
            connected += 1
        else:
            errors += 1
        c.disconnect()

    clients = []
    for i in range(count):
        c = mqtt.Client(client_id=client_id("storm", i),
                        callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        c.on_connect = on_connect
        try:
            c.connect(host, port, keepalive=10)
            c.loop_start()
        except Exception as e:
            errors += 1
        clients.append(c)

    # Wait for all to finish
    for c in clients:
        for _ in range(50):
            c.loop(timeout=0.1)
        c.loop_stop()

    elapsed = time.time() - start
    rate = count / elapsed if elapsed > 0 else 0
    print(f"  Connected: {connected}, Errors: {errors}")
    print(f"  Time: {elapsed:.2f}s, Rate: {rate:.0f} conn/s")
    report("connection_storm_rate", f"{rate:.0f}", "conn/s")
    report("connection_storm_time", f"{elapsed:.2f}", "s")
    return connected, errors

def publish_storm(host, port, num_clients, msgs_per_client):
    """Measure publish throughput."""
    print(f"\n=== Publish Storm: {num_clients} clients x {msgs_per_client} msgs ===")
    total_msgs = num_clients * msgs_per_client
    sent = [0] * num_clients
    errors = [0] * num_clients
    barrier = threading.Barrier(num_clients + 1)

    def publish_worker(idx):
        pid = client_id("pub", idx)
        c = mqtt.Client(client_id=pid,
                        callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        c.connect(host, port, keepalive=30)
        c.loop_start()
        barrier.wait()  # sync start

        for _ in range(msgs_per_client):
            try:
                info = c.publish(f"stress/pub/{idx}",
                                 f"msg_{idx}_{_}".encode() * 10)
                if info.rc == 0:
                    sent[idx] += 1
                else:
                    errors[idx] += 1
            except:
                errors[idx] += 1

        barrier.wait()  # wait for all to finish publish loop
        c.loop_stop()
        c.disconnect()

    threads = []
    for i in range(num_clients):
        t = threading.Thread(target=publish_worker, args=(i,))
        t.start()
        threads.append(t)

    barrier.wait()  # let all clients start publishing
    start = time.time()
    barrier.wait()  # all done publishing
    elapsed = time.time() - start

    for t in threads:
        t.join()

    total_sent = sum(sent)
    total_err = sum(errors)
    rate = total_sent / elapsed if elapsed > 0 else 0
    print(f"  Sent: {total_sent}/{total_msgs}, Errors: {total_err}")
    print(f"  Time: {elapsed:.2f}s, Throughput: {rate:.0f} msg/s")
    report("publish_throughput", f"{rate:.0f}", "msg/s")
    return total_sent, total_err

def sub_pub_accuracy(host, port, num_publishers, msgs_per_pub):
    """One subscriber on '#', N publishers. Verify all messages received."""
    print(f"\n=== Delivery Accuracy: {num_publishers} publishers x {msgs_per_pub} msgs ===")
    total_msgs = num_publishers * msgs_per_pub
    received = []
    recv_lock = threading.Lock()
    pub_errors = [0] * num_publishers
    subscriber_ready = threading.Event()

    def on_sub_message(client, userdata, msg):
        with recv_lock:
            received.append(msg)

    # Subscriber
    sub_id = client_id("sub", 0)
    sub = mqtt.Client(client_id=sub_id,
                      callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
    sub.on_message = on_sub_message

    def on_sub_connect(c, ud, flags, rc, props=None):
        c.subscribe("#")
        subscriber_ready.set()

    sub.on_connect = on_sub_connect
    sub.connect(host, port, keepalive=60)
    sub.loop_start()
    subscriber_ready.wait(timeout=5)

    # Publishers
    def pub_worker(idx):
        pid = client_id("acc", idx)
        c = mqtt.Client(client_id=pid,
                        callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        c.connect(host, port, keepalive=30)
        c.loop_start()
        for _ in range(msgs_per_pub):
            try:
                c.publish(f"stress/acc/{idx}",
                          f"verify_{idx}_{_}".encode())
            except:
                pub_errors[idx] += 1
        c.loop_stop()
        c.disconnect()

    pub_threads = []
    pub_start = time.time()
    for i in range(num_publishers):
        t = threading.Thread(target=pub_worker, args=(i,))
        t.start()
        pub_threads.append(t)

    for t in pub_threads:
        t.join()
    pub_elapsed = time.time() - pub_start

    # Give subscriber time to receive
    time.sleep(2)
    sub.loop_stop()
    sub.disconnect()

    delivered = len(received)
    pct = 100.0 * delivered / total_msgs if total_msgs > 0 else 0
    print(f"  Published: {total_msgs}, Delivered: {delivered} ({pct:.1f}%)")
    print(f"  Publish time: {pub_elapsed:.2f}s, Pub rate: {total_msgs/pub_elapsed:.0f} msg/s")
    report("delivery_accuracy", f"{pct:.1f}", "%")
    report("publish_rate", f"{total_msgs/pub_elapsed:.0f}", "msg/s")
    return delivered, total_msgs

def concurrent_connect(host, port, count):
    """Connect `count` clients simultaneously."""
    print(f"\n=== Concurrent Connect: {count} clients ===")
    connected = [False] * count
    errors = [0] * count

    def worker(idx):
        cid = client_id("conc", idx)
        c = mqtt.Client(client_id=cid,
                        callback_api_version=mqtt.CallbackAPIVersion.VERSION2)
        def on_connect(client, ud, flags, rc, props=None):
            if rc == 0:
                connected[idx] = True
            else:
                errors[idx] += 1
            client.disconnect()
        c.on_connect = on_connect
        try:
            c.connect(host, port, keepalive=10)
            c.loop_start()
            time.sleep(2)
            c.loop_stop()
        except:
            errors[idx] += 1

    threads = []
    start = time.time()
    for i in range(count):
        t = threading.Thread(target=worker, args=(i,))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()
    elapsed = time.time() - start

    ok = sum(connected)
    err = sum(e for e in errors)
    rate = ok / elapsed if elapsed > 0 else 0
    print(f"  Connected: {ok}/{count}, Errors: {err}")
    print(f"  Time: {elapsed:.2f}s, Rate: {rate:.0f} conn/s")
    report("concurrent_connect_rate", f"{rate:.0f}", "conn/s")

def main():
    parser = argparse.ArgumentParser(description="MQTT Broker Stress Test")
    parser.add_argument("host", nargs="?", default="127.0.0.1")
    parser.add_argument("port", nargs="?", type=int, default=1883)
    parser.add_argument("--connections", type=int, default=100,
                        help="Number of connections for storm test")
    parser.add_argument("--clients", type=int, default=10,
                        help="Number of clients for publish/sub tests")
    parser.add_argument("--messages", type=int, default=100,
                        help="Messages per client")
    args = parser.parse_args()

    print(f"MQTT Broker Stress Test — {args.host}:{args.port}")
    print(f"  Connection storm: {args.connections} clients")
    print(f"  Publish storm: {args.clients} clients x {args.messages} msgs")
    print(f"  Delivery accuracy: {args.clients} pubs x {args.messages} msgs")

    # Run tests
    connection_storm(args.host, args.port, min(args.connections, 500))
    concurrent_connect(args.host, args.port, min(args.clients, 50))
    sub_pub_accuracy(args.host, args.port, args.clients, args.messages)
    publish_storm(args.host, args.port, args.clients, args.messages)

    # Summary
    print("\n" + "=" * 50)
    print("RESULTS SUMMARY")
    print("=" * 50)
    for name, (val, unit) in sorted(RESULTS.items()):
        print(f"  {name:30s} {val:>12s} {unit}")

if __name__ == "__main__":
    main()
