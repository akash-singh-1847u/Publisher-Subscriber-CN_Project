import socket
import threading
import json

HOST = '0.0.0.0'
PORT = 9000
HEADER_SIZE = 10
FORMAT = "utf-8"

topics = {}
lock = threading.Lock()


def receive_json(conn):
    try:
        header = conn.recv(HEADER_SIZE)
        if not header:
            return None
        msg_length = int(header.decode(FORMAT).strip())
        data = b""
        while len(data) < msg_length:
            packet = conn.recv(msg_length - len(data))
            if not packet:
                return None
            data += packet
        return json.loads(data.decode(FORMAT))
    except:
        return None


def handle_client(conn, addr):
    print(f"[CONNECTED] {addr}")
    while True:
        try:
            peek = conn.recv(1, socket.MSG_PEEK)
            if not peek:
                break
            # Publisher sends length-prefixed JSON (header starts with digit/space).
            if peek in b"0123456789 {":
                msg = receive_json(conn)
                if not msg:
                    break

                command = msg.get("cmd")

                if command == "CREATE":
                    topic = msg.get("topic")
                    with lock:
                        if topic not in topics:
                            topics[topic] = []
                    print(f"[CREATE] {topic}")

                elif command == "PUBLISH":
                    topic = msg.get("topic")
                    data = msg.get("data")
                    print(f"[PUBLISH] {topic}: {data}")

                    # Auto-create topic if it doesn't exist yet
                    with lock:
                        if topic not in topics:
                            topics[topic] = []
                        subscribers = list(topics[topic])

                    print(f"[ROUTING] sending to {len(subscribers)} subscribers")
                    for sub in subscribers:
                        try:
                            sub.send(f"{topic}:{data}".encode(FORMAT))
                        except:
                            pass

            else:
                msg = conn.recv(1024).decode(FORMAT)
                if not msg:
                    break

                parts = msg.strip().split(" ", 2)
                command = parts[0]

                if command == "SUBSCRIBE":
                    topic = parts[1]
                    with lock:
                        if topic not in topics:
                            topics[topic] = []
                        if conn not in topics[topic]:
                            topics[topic].append(conn)
                    print(f"[SUBSCRIBE] {addr} → {topic}")

                elif command == "UNSUBSCRIBE":
                    topic = parts[1]
                    with lock:
                        if topic in topics and conn in topics[topic]:
                            topics[topic].remove(conn)
                    print(f"[UNSUBSCRIBE] {addr} → {topic}")

                elif command == "LIST_TOPICS":
                    # Return all known topic names to the requesting subscriber
                    with lock:
                        topic_list = list(topics.keys())
                    response = "TOPICS:" + ",".join(topic_list) + "\n"
                    try:
                        conn.send(response.encode(FORMAT))
                    except:
                        pass
                    print(f"[LIST_TOPICS] sent {len(topic_list)} topics to {addr}")

        except:
            break

    with lock:
        for topic in topics:
            if conn in topics[topic]:
                topics[topic].remove(conn)
    conn.close()
    print(f"[DISCONNECTED] {addr}")


def start_broker():
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    server.bind((HOST, PORT))
    server.listen()
    print(f"[BROKER RUNNING] {HOST}:{PORT}")
    while True:
        conn, addr = server.accept()
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()


if __name__ == "__main__":
    start_broker()