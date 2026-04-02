import socket
import threading
import json
import time

HOST = "localhost"
PORT = 9000
FORMAT = "utf-8"
HEADER_SIZE = 10

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

def receive_messages(sock):
    """Background thread: continuously listen for incoming messages."""
    print("[LISTENING] Waiting for messages...\n")
    while True:
        try:
            data = sock.recv(1024).decode(FORMAT)
            if not data:
                print("[DISCONNECTED] Broker closed the connection.")
                break
            # Broker sends: "topic:message"
            if ":" in data:
                topic, message = data.split(":", 1)
                timestamp = time.strftime("%H:%M:%S")

                content = f"📨 [{topic.upper()}] {message}"
                width = len(content) + 4

                print("\n" + "═" * width)
                print(f"║ {content} ║")
                print(f"║ ⏰ {timestamp}{' ' * (width - len(timestamp) - 5)}║")
                print("═" * width + "\n")


                print("Choice: ", end="", flush=True)  # Re-prompt
        except Exception as e:
            print(f"[ERROR] {e}")
            break

def show_menu():
    print("\n==== Subscriber Menu ====")
    print("1. Subscribe to topic")
    print("2. Unsubscribe from topic")
    print("3. Show available topics")
    print("4. Exit")

def main():
    sock = connect()
    subscribed_topics = []

    available_topics = ["sports", "tech", "finance", "weather"]

    # Start background listener thread
    listener = threading.Thread(target=receive_messages, args=(sock,), daemon=True)
    listener.start()

    while True:
        show_menu()
        choice = input("Choice: ").strip()

        if choice == "1":
            print(f"Available topics: {available_topics}")
            topic = input("Enter topic to subscribe: ").strip()
            if topic not in subscribed_topics:
                subscribe(sock, topic)
                subscribed_topics.append(topic)
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
            print(f"[INFO] Available topics : {available_topics}")
            print(f"[INFO] Your subscriptions: {subscribed_topics}")

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