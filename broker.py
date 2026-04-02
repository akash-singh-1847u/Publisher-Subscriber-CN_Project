import socket       # For creating TCP network connections
import threading    # For handling multiple clients at the same time
import json         # For parsing and building JSON messages

# ─── Server Configuration ───────────────────────────────────────────────────
HOST = '0.0.0.0'    # Listen on all available network interfaces
PORT = 9000         # Port number clients will connect to
HEADER_SIZE = 10    # First 10 bytes of every JSON message = the message length
FORMAT = "utf-8"    # Character encoding for all send/receive operations

# ─── Shared State ────────────────────────────────────────────────────────────
# topics holds all active topics and their subscribers
# Structure: { "topic_name": [conn1, conn2, ...], ... }
topics = {}

# Lock prevents race conditions when multiple threads read/write `topics`
lock = threading.Lock()


# ─────────────────────────────────────────────────────────────────────────────
# receive_json()
# Reads a length-prefixed JSON message from a connection.
# Protocol: [10-byte length header][JSON payload]
# Example:  "0000000023{"cmd":"PUBLISH","topic":"news"}"
# ─────────────────────────────────────────────────────────────────────────────
def receive_json(conn):
    try:
        # Step 1: Read the fixed 10-byte header to find out how long the message is
        header = conn.recv(HEADER_SIZE)
        if not header:
            return None  # Client disconnected before sending anything

        # Step 2: Convert header bytes like b"0000000023" → integer 23
        msg_length = int(header.decode(FORMAT).strip())

        # Step 3: Read the actual message body in chunks
        # (TCP may split large messages across multiple recv() calls)
        data = b""
        while len(data) < msg_length:
            packet = conn.recv(msg_length - len(data))  # Only request what's still missing
            if not packet:
                return None  # Connection dropped mid-message
            data += packet   # Append the received chunk

        # Step 4: Decode the complete bytes and parse into a Python dictionary
        return json.loads(data.decode(FORMAT))

    except:
        return None  # Return None on any error so the caller can handle it cleanly


# ─────────────────────────────────────────────────────────────────────────────
# handle_client()
# Runs in its own thread for every connected client.
# Detects whether incoming data is JSON (publisher) or plain text (subscriber)
# and routes it to the appropriate handler.
# ─────────────────────────────────────────────────────────────────────────────
def handle_client(conn, addr):
    print(f"[CONNECTED] {addr}")

    while True:
        try:
            # Peek at the first byte WITHOUT removing it from the buffer
            # This lets us decide which protocol the client is using
            peek = conn.recv(1, socket.MSG_PEEK)
            if not peek:
                break  # Empty peek = client has disconnected

            # ── JSON Protocol (Publishers) ────────────────────────────────
            # If first byte is a digit or '{', it's a JSON message
            # Digits → length-prefixed format ("0000000042{...}")
            # '{' → raw JSON (fallback)
            if peek in b"0123456789 {":
                msg = receive_json(conn)
                if not msg:
                    break  # Failed to read a valid message

                command = msg.get("cmd")  # Extract the command field from JSON

                # ── CREATE: Register a new topic ──────────────────────────
                if command == "CREATE":
                    topic = msg.get("topic")
                    with lock:  # Acquire lock before modifying shared `topics`
                        if topic not in topics:
                            topics[topic] = []  # Initialize with empty subscriber list
                    print(f"[CREATE] {topic}")

                # ── PUBLISH: Broadcast a message to all topic subscribers ──
                elif command == "PUBLISH":
                    topic = msg.get("topic")
                    data = msg.get("data")
                    print(f"[PUBLISH] {topic}: {data}")

                    with lock:
                        if topic not in topics:
                            topics[topic] = []          # Auto-create topic if it wasn't pre-created
                        subscribers = list(topics[topic])  # Snapshot the list to avoid holding the lock while sending

                    print(f"[ROUTING] sending to {len(subscribers)} subscribers")

                    # Send the message to every subscriber on this topic
                    for sub in subscribers:
                        try:
                            # Format: "topic_name:message_data"
                            sub.send(f"{topic}:{data}".encode(FORMAT))
                        except:
                            pass  # Skip dead/broken connections silently

            # ── Plain-Text Protocol (Subscribers) ─────────────────────────
            # Commands sent as simple space-separated strings:
            # "SUBSCRIBE sports"
            # "UNSUBSCRIBE sports"
            # "LIST_TOPICS"
            else:
                msg = conn.recv(1024).decode(FORMAT)  # Read up to 1024 bytes
                if not msg:
                    break

                # Split into at most 3 parts: ["COMMAND", "topic", "optional_extra"]
                parts = msg.strip().split(" ", 2)
                command = parts[0]

                # ── SUBSCRIBE: Add this client to a topic's subscriber list ──
                if command == "SUBSCRIBE":
                    topic = parts[1]
                    with lock:
                        if topic not in topics:
                            topics[topic] = []       # Create topic if it doesn't exist yet
                        if conn not in topics[topic]:
                            topics[topic].append(conn)  # Register this connection as a subscriber
                    print(f"[SUBSCRIBE] {addr} → {topic}")

                # ── UNSUBSCRIBE: Remove this client from a topic ───────────
                elif command == "UNSUBSCRIBE":
                    topic = parts[1]
                    with lock:
                        if topic in topics and conn in topics[topic]:
                            topics[topic].remove(conn)  # Remove without affecting other subscribers
                    print(f"[UNSUBSCRIBE] {addr} → {topic}")

                # ── LIST_TOPICS: Return all known topic names ──────────────
                elif command == "LIST_TOPICS":
                    with lock:
                        topic_list = list(topics.keys())  # Grab a snapshot of all topic names

                    # Send back a comma-separated list: "TOPICS:sports,news,chat\n"
                    response = "TOPICS:" + ",".join(topic_list) + "\n"
                    try:
                        conn.send(response.encode(FORMAT))
                    except:
                        pass
                    print(f"[LIST_TOPICS] sent {len(topic_list)} topics to {addr}")

        except:
            break  # Exit loop on any unhandled exception and proceed to cleanup

    # ── Cleanup on Disconnect ─────────────────────────────────────────────────
    # When a client disconnects (normally or due to error),
    # remove it from every topic it was subscribed to
    with lock:
        for topic in topics:
            if conn in topics[topic]:
                topics[topic].remove(conn)

    conn.close()  # Close the socket
    print(f"[DISCONNECTED] {addr}")


# ─────────────────────────────────────────────────────────────────────────────
# start_broker()
# Sets up the TCP server socket and enters an infinite accept loop.
# Each new client gets its own thread via handle_client().
# ─────────────────────────────────────────────────────────────────────────────
def start_broker():
    # Create a TCP/IPv4 socket
    server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # SO_REUSEADDR: allows the server to restart immediately after stopping
    # without waiting for the OS to release the port
    server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    server.bind((HOST, PORT))   # Bind the socket to the host and port
    server.listen()             # Start listening for incoming connections
    print(f"[BROKER RUNNING] {HOST}:{PORT}")

    while True:
        # Block here until a client connects, then get their socket + address
        conn, addr = server.accept()

        # Spin up a dedicated thread for this client so others aren't blocked
        thread = threading.Thread(target=handle_client, args=(conn, addr))
        thread.start()


# Entry point — only runs when executed directly, not when imported as a module
if __name__ == "__main__":
    start_broker()