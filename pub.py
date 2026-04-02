import socket
import json
import time
import random
import threading

HEADER_SIZE = 10
FORMAT = "utf-8"

# Default seed topics — more can be created at runtime
DEFAULT_TOPICS = ["sports", "tech", "finance", "weather"]

<<<<<<< HEAD
=======

>>>>>>> fd758cb (all 3 updated)
def connect(host="localhost", port=9000):
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.connect((host, port))
    print("Connected to broker")
    return sock


def send(sock, data, lock):
    msg = json.dumps(data).encode(FORMAT)
    header = f"{len(msg):<{HEADER_SIZE}}".encode(FORMAT)
    with lock:
        sock.sendall(header + msg)


def create_topic(sock, topic, lock):
    send(sock, {"cmd": "CREATE", "topic": topic}, lock)


def publish(sock, topic, message, lock):
    send(sock, {
        "cmd": "PUBLISH",
        "topic": topic,
        "data": message,
        "timestamp": time.time()
    }, lock)


<<<<<<< HEAD
# Default topics (same as subscriber)
TOPICS = ["sports", "tech", "finance", "weather"]


def random_news():
=======
def random_news(topics):
>>>>>>> fd758cb (all 3 updated)
    news_data = {
        "sports":  ["India won the match", "Messi scored a hattrick", "Olympics announced new events"],
        "tech":    ["New AI model released", "Quantum computing breakthrough", "New smartphone launched"],
        "finance": ["Stock market hits record high", "Crypto prices surge", "Interest rates increased"],
        "weather": ["Heavy rains expected tomorrow", "Heatwave alert issued", "Cyclone approaching coast"],
    }
<<<<<<< HEAD

    topic = random.choice(TOPICS)
    msg = random.choice(news_data[topic])

    return topic, msg


def auto_publish(sock, lock, delay=2):
=======
    topic = random.choice(topics)
    # For topics not in the preset data, generate a generic message
    msg = random.choice(news_data.get(topic, [f"Update on {topic}"]))
    return topic, msg


def auto_publish(sock, lock, topics, delay=2):
>>>>>>> fd758cb (all 3 updated)
    print("[AUTO MODE] Streaming news... Ctrl+C to stop\n")
    try:
        while True:
            topic, msg = random_news(topics)
            publish(sock, topic, msg, lock)
            print(f"[AUTO] {topic} → {msg}")
            time.sleep(delay)
    except KeyboardInterrupt:
        print("\n[!] Auto mode stopped")


<<<<<<< HEAD
def stress_test(sock, lock, n=1000):
=======
def stress_test(sock, lock, topics, n=1000):
>>>>>>> fd758cb (all 3 updated)
    print(f"[STRESS TEST] Sending {n} messages...")
    start = time.time()
    threads = []
    for _ in range(n):
        topic, msg = random_news(topics)
        t = threading.Thread(target=publish, args=(sock, topic, msg, lock))
        t.start()
        threads.append(t)
    for t in threads:
        t.join()
    end = time.time()
    print(f"Time: {end - start:.4f}s")
<<<<<<< HEAD
    print(f"Throughput: {n/(end-start):.2f} msgs/sec")
=======
    print(f"Throughput: {n / (end - start):.2f} msgs/sec")
>>>>>>> fd758cb (all 3 updated)


def main():
    sock = connect()
    lock = threading.Lock()

<<<<<<< HEAD
    # Create predefined topics once
    for t in TOPICS:
=======
    # Start with default topics and register them on the broker
    active_topics = list(DEFAULT_TOPICS)
    for t in active_topics:
>>>>>>> fd758cb (all 3 updated)
        create_topic(sock, t, lock)

    while True:
        print("\n==== Publisher Menu ====")
        print(f"Active topics: {active_topics}")
        print("1. Manual Publish")
        print("2. Create New Topic")
        print("3. Auto News Stream")
        print("4. Stress Test")
        print("5. Exit")

        choice = input("Choice: ").strip()

        if choice == "1":
<<<<<<< HEAD
            print(f"Available topics: {TOPICS}")
            topic = input("Choose topic: ").strip()

            if topic not in TOPICS:
                print("[ERROR] Invalid topic. Choose from default list.")
                continue

            msg = input("Message: ")
            publish(sock, topic, msg, lock)

        elif choice == "2":
            auto_publish(sock, lock)

        elif choice == "3":
            n = int(input("Number of messages: "))
            stress_test(sock, lock, n)
=======
            print(f"Active topics: {active_topics}")
            topic = input("Enter topic name (existing or new): ").strip()
            if not topic:
                print("[ERROR] Topic name cannot be empty.")
                continue
            # Auto-create if it's a new topic
            if topic not in active_topics:
                create_topic(sock, topic, lock)
                active_topics.append(topic)
                print(f"[INFO] New topic '{topic}' created.")
            msg = input("Message: ").strip()
            if msg:
                publish(sock, topic, msg, lock)
                print(f"[SENT] {topic} → {msg}")

        elif choice == "2":
            topic = input("New topic name: ").strip()
            if not topic:
                print("[ERROR] Topic name cannot be empty.")
            elif topic in active_topics:
                print(f"[INFO] Topic '{topic}' already exists.")
            else:
                create_topic(sock, topic, lock)
                active_topics.append(topic)
                print(f"[CREATED] Topic '{topic}' is now active.")

        elif choice == "3":
            auto_publish(sock, lock, active_topics)
>>>>>>> fd758cb (all 3 updated)

        elif choice == "4":
            n = int(input("Number of messages: "))
            stress_test(sock, lock, active_topics, n)

        elif choice == "5":
            sock.close()
            break

        else:
            print("[!] Invalid choice")


if __name__ == "__main__":
    main()