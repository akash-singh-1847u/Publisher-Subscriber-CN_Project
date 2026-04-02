# ════════════════════════════════════════════════════════════════════════════
#  broker.py  —  Secure Pub/Sub Message Broker
#  Uses raw TCP sockets + SSL/TLS encryption.
#  Publishers speak JSON; Subscribers speak plain text.
#  Both share the same port — protocol is auto-detected per connection.
# ════════════════════════════════════════════════════════════════════════════

import socket       # Low-level TCP socket creation and management
import threading    # Spawn one thread per client for concurrency
import json         # Serialize/deserialize publisher messages
import ssl          # Wrap sockets with TLS encryption
import time         # Timestamps for latency and uptime tracking
import logging      # Structured, timestamped logs (replaces bare print)


# ─── Section 1: Logging ───────────────────────────────────────────────────────
# Sets up a logger that prints:  "10:32:15 [INFO] [CONNECTED] 127.0.0.1"
# All broker output goes through this instead of print() so it's consistent.
logging.basicConfig(
    level=logging.INFO,                          # Show INFO and above (not DEBUG)
    format="%(asctime)s [%(levelname)s] %(message)s",
    datefmt="%H:%M:%S"
)
log = logging.getLogger("broker")               # Named logger for this module


# ─── Section 2: Configuration Constants ──────────────────────────────────────
HOST        = '0.0.0.0'   # Bind to ALL interfaces (not just localhost)
PORT        = 9000         # Both publishers and subscribers connect here
HEADER_SIZE = 10           # Every JSON message starts with a 10-byte length header
FORMAT      = "utf-8"      # Encoding for all bytes ↔ string conversions

# SSL certificate files — generated once with openssl, loaded on every startup
CERTFILE = "server.crt"   # Public certificate sent to clients during handshake
KEYFILE  = "server.key"   # Private key — NEVER shared, stays on the server


# ─── Section 3: Shared State ──────────────────────────────────────────────────
# `topics` is the core data structure of the broker.
# Key   = topic name (string)
# Value = list of SSL socket objects for every subscriber on that topic
# Example: { "sports": [<SSLSocket A>, <SSLSocket B>], "tech": [<SSLSocket C>] }
topics = {}

# `lock` protects `topics` from race conditions.
# Without it, two threads modifying the list simultaneously could corrupt it.
lock = threading.Lock()


# ─── Section 4: Performance Counters ─────────────────────────────────────────
# Tracks real-time broker metrics, reported when a subscriber sends "STATS".
perf = {
    "messages_published": 0,    # Total PUBLISH commands received by broker
    "messages_delivered": 0,    # Total successful sends to subscribers
    "clients_connected":  0,    # Current number of live client connections
    "start_time":         time.time(),  # Broker startup time (for uptime calc)
}
perf_lock = threading.Lock()    # Separate lock so perf updates never block topics


# ════════════════════════════════════════════════════════════════════════════
#  FUNCTION: receive_json(conn)
#
#  PURPOSE:
#    Read exactly one complete JSON message from an SSL socket.
#
#  WHY A LOOP?
#    TCP is a stream — it has no concept of message boundaries.
#    One send() on the publisher side may arrive as multiple recv() chunks.
#    The 10-byte header tells us the total length, and we loop until we
#    have accumulated exactly that many bytes.
#
#  WIRE FORMAT:
#    [ 10 bytes: ASCII length, left-justified, space-padded ]
#    [ N bytes:  UTF-8 JSON payload                         ]
#
#  Example on the wire:
#    b"31        " + b'{"cmd":"PUBLISH","topic":"sports"}'
#                     ↑ header says 31 bytes  ↑ payload is 31 bytes
# ════════════════════════════════════════════════════════════════════════════
def receive_json(conn):
    try:
        # ── Step 1: Read the 10-byte header ──────────────────────────────────
        header = conn.recv(HEADER_SIZE)
        if not header:
            return None         # Empty header = client disconnected cleanly

        # ── Step 2: Convert "31        " → integer 31 ────────────────────────
        try:
            msg_length = int(header.decode(FORMAT).strip())
        except ValueError:
            log.warning("Malformed header — dropping message")
            return None

        # ── Step 3: Sanity-check the declared size ────────────────────────────
        # Reject 0-byte or unreasonably large messages (max 1 MB)
        # This prevents memory exhaustion from malicious or buggy senders.
        if msg_length <= 0 or msg_length > 1_000_000:
            log.warning(f"Suspicious message length {msg_length} — dropping")
            return None

        # ── Step 4: Read payload in chunks until all bytes are received ───────
        data = b""
        while len(data) < msg_length:
            # Only request the bytes still missing — avoids over-reading
            packet = conn.recv(msg_length - len(data))
            if not packet:
                return None     # Connection dropped mid-message
            data += packet      # Accumulate chunks

        # ── Step 5: Decode bytes → JSON string → Python dict ─────────────────
        return json.loads(data.decode(FORMAT))

    except json.JSONDecodeError:
        log.warning("Invalid JSON payload — dropping message")
        return None
    except ssl.SSLError as e:
        log.error(f"SSL error during read: {e}")
        return None
    except Exception as e:
        log.error(f"Unexpected error in receive_json: {e}")
        return None


# ════════════════════════════════════════════════════════════════════════════
#  FUNCTION: handle_client(conn, addr)
#
#  PURPOSE:
#    Runs in its own thread for every connected client.
#    Detects whether the client is a Publisher (JSON) or Subscriber (text)
#    by peeking at the first byte, then dispatches accordingly.
#
#  PROTOCOL DETECTION (MSG_PEEK trick):
#    Publishers send:   "31        {"cmd":"PUBLISH"...}"
#                        ↑ first byte is always a digit (the length header)
#    Subscribers send:  "SUBSCRIBE sports\n"
#                        ↑ first byte is always a letter
#
#    socket.MSG_PEEK reads a byte WITHOUT removing it from the buffer,
#    so whichever branch runs next still sees the full original data.
# ════════════════════════════════════════════════════════════════════════════
def handle_client(conn, addr):
    # Increment the active client counter when this thread starts
    with perf_lock:
        perf["clients_connected"] += 1

    log.info(f"[CONNECTED] {addr}  (active clients: {perf['clients_connected']})")

    while True:
        try:
            # ── Peek at first byte — doesn't consume it ───────────────────────
            peek = conn.recv(1, socket.MSG_PEEK)
            if not peek:
                break   # Client closed the connection gracefully


            # ════════════════════════════════════════════════════════════════════
            #  PUBLISHER PATH — JSON Protocol
            #  Activated when first byte is a digit (length header) or '{'
            # ════════════════════════════════════════════════════════════════════
            if peek in b"0123456789 {":
                msg = receive_json(conn)
                if msg is None:
                    break       # Bad data or disconnection — exit loop

                command = msg.get("cmd", "").upper()   # e.g. "CREATE" or "PUBLISH"

                # ── CREATE ────────────────────────────────────────────────────
                # Registers a topic on the broker with an empty subscriber list.
                # Publishers call this before publishing to ensure the topic exists.
                if command == "CREATE":
                    topic = msg.get("topic", "").strip()
                    if not topic:
                        log.warning("[CREATE] Empty topic name — ignored")
                        continue
                    with lock:
                        if topic not in topics:
                            topics[topic] = []                  # New empty subscriber list
                            log.info(f"[CREATE] New topic: '{topic}'")
                        else:
                            log.info(f"[CREATE] Topic '{topic}' already exists")

                # ── PUBLISH ───────────────────────────────────────────────────
                # The main broker operation: receive a message and fan it out
                # to every subscriber currently registered for that topic.
                elif command == "PUBLISH":
                    topic   = msg.get("topic", "").strip()
                    data    = msg.get("data", "")
                    # Publisher embeds its send timestamp so we can measure latency
                    sent_at = msg.get("timestamp", time.time())

                    if not topic:
                        log.warning("[PUBLISH] Empty topic name — ignored")
                        continue

                    # Publisher → broker latency in milliseconds
                    latency_ms = (time.time() - sent_at) * 1000

                    with perf_lock:
                        perf["messages_published"] += 1     # Count this publish

                    with lock:
                        if topic not in topics:
                            topics[topic] = []              # Auto-create if not pre-registered

                        # IMPORTANT: Copy the list before releasing the lock.
                        # This lets us send to subscribers outside the lock,
                        # so slow subscribers don't delay other threads.
                        subscribers = list(topics[topic])

                    log.info(f"[PUBLISH] '{topic}' | {len(subscribers)} subscribers | "
                             f"latency {latency_ms:.2f} ms")

                    delivered = 0       # Count successful deliveries
                    dead_subs = []      # Collect broken connections to remove later

                    # Fan-out loop: send to every subscriber independently
                    for sub in subscribers:
                        try:
                            # Delivery format: "topic:data\n"
                            # The newline acts as a message delimiter for subscribers
                            sub.send(f"{topic}:{data}\n".encode(FORMAT))
                            delivered += 1
                        except Exception as e:
                            # This subscriber's socket is broken — queue for removal
                            log.warning(f"[DELIVER] Failed → marking dead: {e}")
                            dead_subs.append(sub)

                    # Remove dead subscribers (done after the loop to stay safe)
                    if dead_subs:
                        with lock:
                            for dead in dead_subs:
                                if dead in topics.get(topic, []):
                                    topics[topic].remove(dead)

                    with perf_lock:
                        perf["messages_delivered"] += delivered

                    log.info(f"[ROUTED] '{topic}' → {delivered}/{len(subscribers)} delivered")

                else:
                    log.warning(f"[UNKNOWN CMD] {command} — ignored")


            # ════════════════════════════════════════════════════════════════════
            #  SUBSCRIBER PATH — Plain-text Protocol
            #  Commands arrive as newline-separated ASCII strings:
            #    "SUBSCRIBE sports\n"
            #    "UNSUBSCRIBE sports\n"
            #    "LIST_TOPICS\n"
            #    "STATS\n"
            # ════════════════════════════════════════════════════════════════════
            else:
                raw = conn.recv(1024).decode(FORMAT)
                if not raw:
                    break

                # A single recv() may contain multiple commands (e.g. rapid UI clicks),
                # so split on newlines and process each one individually.
                for line in raw.strip().split("\n"):
                    line = line.strip()
                    if not line:
                        continue

                    parts   = line.split(" ", 2)        # Max 3 parts: CMD topic extra
                    command = parts[0].upper()

                    # ── SUBSCRIBE ─────────────────────────────────────────────
                    # Adds this socket to the named topic's subscriber list.
                    # From now on, any PUBLISH to that topic will reach this client.
                    if command == "SUBSCRIBE":
                        if len(parts) < 2:
                            log.warning("[SUBSCRIBE] Missing topic name")
                            continue
                        topic = parts[1].strip()
                        if not topic:
                            continue
                        with lock:
                            if topic not in topics:
                                topics[topic] = []      # Create topic on first subscription
                            if conn not in topics[topic]:
                                topics[topic].append(conn)  # Register this connection
                        log.info(f"[SUBSCRIBE] {addr} → '{topic}'")

                    # ── UNSUBSCRIBE ───────────────────────────────────────────
                    # Removes this socket from the topic's subscriber list.
                    # Future publishes to that topic will no longer reach this client.
                    elif command == "UNSUBSCRIBE":
                        if len(parts) < 2:
                            log.warning("[UNSUBSCRIBE] Missing topic name")
                            continue
                        topic = parts[1].strip()
                        with lock:
                            if topic in topics and conn in topics[topic]:
                                topics[topic].remove(conn)
                        log.info(f"[UNSUBSCRIBE] {addr} ✗ '{topic}'")

                    # ── LIST_TOPICS ───────────────────────────────────────────
                    # Returns all registered topic names to the requesting subscriber.
                    # Response format: "TOPICS:sports,tech,finance\n"
                    elif command == "LIST_TOPICS":
                        with lock:
                            topic_list = list(topics.keys())    # Snapshot topic names
                        response = "TOPICS:" + ",".join(topic_list) + "\n"
                        try:
                            conn.send(response.encode(FORMAT))
                        except Exception as e:
                            log.warning(f"[LIST_TOPICS] Send failed: {e}")
                        log.info(f"[LIST_TOPICS] {len(topic_list)} topics → {addr}")

                    # ── STATS ─────────────────────────────────────────────────
                    # Returns live broker performance metrics to the requesting client.
                    # Response format: "STATS:uptime=42.1s,published=500,...\n"
                    # throughput = total messages published / seconds since start
                    elif command == "STATS":
                        with perf_lock:
                            uptime  = time.time() - perf["start_time"]
                            pub     = perf["messages_published"]
                            deliv   = perf["messages_delivered"]
                            clients = perf["clients_connected"]
                        throughput = pub / uptime if uptime > 0 else 0
                        stats_msg = (
                            f"STATS:"
                            f"uptime={uptime:.1f}s,"
                            f"published={pub},"
                            f"delivered={deliv},"
                            f"clients={clients},"
                            f"throughput={throughput:.2f}msg/s\n"
                        )
                        try:
                            conn.send(stats_msg.encode(FORMAT))
                        except Exception as e:
                            log.warning(f"[STATS] Send failed: {e}")

                    else:
                        log.warning(f"[UNKNOWN CMD] '{command}' from {addr}")

        # ── Per-iteration error handling ──────────────────────────────────────
        except ssl.SSLError as e:
            # TLS-level errors (e.g. client sent non-TLS data, cert expired)
            log.error(f"[SSL ERROR] {addr}: {e}")
            break
        except ConnectionResetError:
            # Client process crashed or killed without closing the socket
            log.warning(f"[RESET] {addr} forcibly closed the connection")
            break
        except Exception as e:
            log.error(f"[ERROR] Unexpected in handle_client for {addr}: {e}")
            break

    # ── Cleanup when the loop exits (for any reason) ──────────────────────────
    # Remove this socket from every topic it was subscribed to.
    # Without this, dead connections would pile up and cause delivery failures.
    with lock:
        for topic in topics:
            if conn in topics[topic]:
                topics[topic].remove(conn)

    with perf_lock:
        # max(0, ...) guards against any accidental double-decrement
        perf["clients_connected"] = max(0, perf["clients_connected"] - 1)

    try:
        conn.close()    # Release the OS socket resource
    except Exception:
        pass            # Already closed — ignore

    log.info(f"[DISCONNECTED] {addr}  (active clients: {perf['clients_connected']})")


# ════════════════════════════════════════════════════════════════════════════
#  FUNCTION: build_ssl_context()
#
#  PURPOSE:
#    Build the server-side SSL/TLS configuration object.
#    This is called once at startup and reused for every connection.
#
#  WHY TLS 1.2 minimum?
#    TLS 1.0 and 1.1 have known vulnerabilities (BEAST, POODLE).
#    Enforcing 1.2+ disables those older protocol versions entirely.
# ════════════════════════════════════════════════════════════════════════════
def build_ssl_context():
    # PROTOCOL_TLS_SERVER = server mode (as opposed to PROTOCOL_TLS_CLIENT)
    context = ssl.SSLContext(ssl.PROTOCOL_TLS_SERVER)

    # Reject TLS 1.0 and 1.1 — only allow 1.2 and 1.3
    context.minimum_version = ssl.TLSVersion.TLSv1_2

    # Load our certificate chain and private key
    # The certificate is sent to clients during the TLS handshake so they
    # can verify they're actually talking to this server (not an impersonator).
    try:
        context.load_cert_chain(certfile=CERTFILE, keyfile=KEYFILE)
    except FileNotFoundError:
        log.critical(
            f"Certificate or key file not found ({CERTFILE} / {KEYFILE}).\n"
            "Generate them with:\n"
            "  openssl req -x509 -newkey rsa:2048 -keyout server.key "
            "-out server.crt -days 365 -nodes -subj '/CN=localhost'"
        )
        raise   # Fatal error — cannot run without SSL files

    return context


# ════════════════════════════════════════════════════════════════════════════
#  FUNCTION: start_broker()
#
#  PURPOSE:
#    Entry point — creates the server socket, applies SSL, and loops forever
#    accepting new client connections, each served in its own thread.
#
#  FLOW:
#    1. Build SSL context (loads cert + key)
#    2. Create raw TCP socket
#    3. Wrap it with SSL → every accepted connection is encrypted from byte 1
#    4. accept() loop → spawn thread → go back to waiting
# ════════════════════════════════════════════════════════════════════════════
def start_broker():
    ssl_context = build_ssl_context()   # Must succeed before we open the port

    # AF_INET   = IPv4
    # SOCK_STREAM = TCP (reliable, ordered, stream-based)
    raw_server = socket.socket(socket.AF_INET, socket.SOCK_STREAM)

    # SO_REUSEADDR: If the broker crashes and restarts quickly, the OS may still
    # hold the port in TIME_WAIT state. This option bypasses that wait.
    raw_server.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)

    raw_server.bind((HOST, PORT))   # Claim the port
    raw_server.listen(50)           # OS queues up to 50 pending connections

    # Wrap the server socket with SSL.
    # From this point, every conn returned by accept() is an SSLSocket —
    # the TLS handshake happens automatically inside accept().
    server = ssl_context.wrap_socket(raw_server, server_side=True)

    log.info(f"[BROKER RUNNING] SSL/TLS on {HOST}:{PORT}")
    log.info(f"[CERT] {CERTFILE}  |  [KEY] {KEYFILE}")

    # ── Main accept loop ──────────────────────────────────────────────────────
    while True:
        try:
            # Block here until a client connects.
            # conn = encrypted SSLSocket for this client
            # addr = (ip_string, port_int) tuple
            conn, addr = server.accept()

        except ssl.SSLError as e:
            # Handshake failed — e.g. client used plain TCP, wrong cert, etc.
            # Log and continue — don't crash the whole broker over one bad client.
            log.warning(f"[SSL HANDSHAKE FAILED] {e}")
            continue

        except KeyboardInterrupt:
            # Ctrl+C — clean shutdown
            log.info("[SHUTDOWN] Broker stopped by user")
            break

        except Exception as e:
            log.error(f"[ACCEPT ERROR] {e}")
            continue

        # Spawn a dedicated thread for this client.
        # daemon=True means if the main thread exits (Ctrl+C), all client
        # threads are automatically killed — no zombie threads left behind.
        thread = threading.Thread(
            target=handle_client,
            args=(conn, addr),
            daemon=True
        )
        thread.start()
        log.info(f"[THREAD] Spawned for {addr}  (total: {threading.active_count()} threads)")


# Only run start_broker() when this file is executed directly.
# If another script does `import broker`, this block is skipped.
if __name__ == "__main__":
    start_broker()