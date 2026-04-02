import socket
import threading
import time

HOST = "localhost"
PORT = 9000
FORMAT = "utf-8"

available_topics = []
topics_lock = threading.Lock()


def connect():
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((HOST, PORT))
    print("[CONNECTED] Connected to broker")
    return sock


def send_command(sock, command: str):
    """Send a plain-text command matching the broker's protocol."""
    sock.sendall(command.encode(FORMAT))


def subscribe(sock, topic: str):
    send_command(sock, f"SUBSCRIBE {topic}")
    print(f"[SUBSCRIBED] → {topic}")


def unsubscribe(sock, topic: str):
    send_command(sock, f"UNSUBSCRIBE {topic}")
    print(f"[UNSUBSCRIBED] → {topic}")


def request_topic_list(sock):
    """Ask the broker for all current topics."""
    send_command(sock, "LIST_TOPICS")


def receive_messages(sock):
    """
    Background thread: continuously listen for incoming messages.
    Handles both TOPICS: responses and topic:message notifications.
    """
    global available_topics
    print("[LISTENING] Waiting for messages...\n")
    buffer = ""

    while True:
        try:
            data = sock.recv(4096).decode(FORMAT)
            if not data:
                print("[DISCONNECTED] Broker closed the connection.")
                break

            buffer += data

            # Process all complete lines in the buffer
            while "\n" in buffer:
                line, buffer = buffer.split("\n", 1)
                line = line.strip()
                if not line:
                    continue

                if line.startswith("TOPICS:"):
                    # Update the live topic list
                    raw = line[len("TOPICS:"):]
                    new_topics = [t for t in raw.split(",") if t]
                    with topics_lock:
                        available_topics = new_topics
                    print(f"\n[TOPICS UPDATED] {available_topics}")
                    print("Choice: ", end="", flush=True)

                elif ":" in line:
                    topic, message = line.split(":", 1)
                    timestamp = time.strftime("%H:%M:%S")

                    content = f"📨 [{topic.upper()}] {message}"
                    width = len(content) + 4

                    print("\n" + "═" * width)
                    print(f"║ {content} ║")
                    print(f"║ ⏰ {timestamp}{' ' * (width - len(timestamp) - 5)}║")
                    print("═" * width + "\n")
                    print("Choice: ", end="", flush=True)

            # Handle remaining buffer content without newline
            if buffer and ":" in buffer and not buffer.startswith("TOPICS:"):
                line = buffer.strip()
                buffer = ""
                if line:
                    topic, message = line.split(":", 1)
                    timestamp = time.strftime("%H:%M:%S")
                    content = f"📨 [{topic.upper()}] {message}"
                    width = len(content) + 4
                    print("\n" + "═" * width)
                    print(f"║ {content} ║")
                    print(f"║ ⏰ {timestamp}{' ' * (width - len(timestamp) - 5)}║")
                    print("═" * width + "\n")
                    print("Choice: ", end="", flush=True)

        except Exception as e:
            print(f"[ERROR] {e}")
            break


def show_menu():
    print("\n==== Subscriber Menu ====")
    print("1. Subscribe to topic")
    print("2. Unsubscribe from topic")
    print("3. Show / Refresh available topics")
    print("4. Exit")


def main():
    global available_topics

    sock = connect()
    subscribed_topics = []

    # Fetch initial topic list from broker
    request_topic_list(sock)

    # Start background listener thread
    listener = threading.Thread(target=receive_messages, args=(sock,), daemon=True)
    listener.start()

    # Give the listener a moment to receive the initial topic list
    time.sleep(0.3)

    while True:
        show_menu()
        choice = input("Choice: ").strip()

        if choice == "1":
            # Refresh topic list before showing options
            request_topic_list(sock)
            time.sleep(0.2)   # brief wait for broker response

            with topics_lock:
                current_topics = list(available_topics)

            print(f"Available topics: {current_topics}")
            topic = input("Enter topic to subscribe (or type a new one): ").strip()
            if not topic:
                print("[ERROR] Topic name cannot be empty.")
                continue

            if topic not in subscribed_topics:
                subscribe(sock, topic)
                subscribed_topics.append(topic)
                # Add to local available list if it's brand new
                with topics_lock:
                    if topic not in available_topics:
                        available_topics.append(topic)
            else:
                print(f"[INFO] Already subscribed to '{topic}'")

        elif choice == "2":
            if not subscribed_topics:
                print("[INFO] You have no active subscriptions.")
            else:
                print(f"Your subscriptions: {subscribed_topics}")
                topic = input("Enter topic to unsubscribe: ").strip()
                if topic in subscribed_topics:
                    unsubscribe(sock, topic)
                    subscribed_topics.remove(topic)
                else:
                    print(f"[INFO] You are not subscribed to '{topic}'")

        elif choice == "3":
            # Explicitly refresh from broker
            request_topic_list(sock)
            time.sleep(0.2)
            with topics_lock:
                current_topics = list(available_topics)
            print(f"[INFO] Available topics (from broker): {current_topics}")
            print(f"[INFO] Your subscriptions          : {subscribed_topics}")

        elif choice == "4":
            print("[EXIT] Disconnecting...")
            for topic in subscribed_topics:
                unsubscribe(sock, topic)
            sock.close()
            break

        else:
            print("[!] Invalid choice. Try again.")


if __name__ == "__main__":
    main()
