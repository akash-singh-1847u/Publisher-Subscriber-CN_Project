import socket
import json
import time
import random
import threading

HEADER_SIZE = 10
FORMAT = "utf-8"


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
    send(sock, {
        "cmd": "CREATE",
        "topic": topic
    }, lock)


def publish(sock, topic, message, lock):
    send(sock, {
        "cmd": "PUBLISH",
        "topic": topic,
        "data": message,
        "timestamp": time.time()
    }, lock)


# Default topics (same as subscriber)
TOPICS = ["sports", "tech", "finance", "weather"]


def random_news():
    news_data = {
        "sports": [
            "India won the match",
            "Messi scored a hattrick",
            "Olympics announced new events"
        ],
        "tech": [
            "New AI model released",
            "Quantum computing breakthrough",
            "New smartphone launched"
        ],
        "finance": [
            "Stock market hits record high",
            "Crypto prices surge",
            "Interest rates increased"
        ],
        "weather": [
            "Heavy rains expected tomorrow",
            "Heatwave alert issued",
            "Cyclone approaching coast"
        ]
    }

    topic = random.choice(TOPICS)
    msg = random.choice(news_data[topic])

    return topic, msg


def auto_publish(sock, lock, delay=2):
    print("[AUTO MODE] Streaming news... Ctrl+C to stop\n")

    try:
        while True:
            topic, msg = random_news()
            publish(sock, topic, msg, lock)
            print(f"[AUTO] {topic} → {msg}")
            time.sleep(delay)
    except KeyboardInterrupt:
        print("\n[!] Auto mode stopped")


def stress_test(sock, lock, n=1000):
    print(f"[STRESS TEST] Sending {n} messages...")

    start = time.time()

    threads = []
    for _ in range(n):
        topic, msg = random_news()
        t = threading.Thread(target=publish, args=(sock, topic, msg, lock))
        t.start()
        threads.append(t)

    for t in threads:
        t.join()

    end = time.time()

    print(f"Time: {end - start:.4f}s")
    print(f"Throughput: {n/(end-start):.2f} msgs/sec")


def main():
    sock = connect()
    lock = threading.Lock()

    # Create predefined topics once
    for t in TOPICS:
        create_topic(sock, t, lock)

    while True:
        print("\n==== Publisher Menu ====")
        print("1. Manual Publish")
        print("2. Auto News Stream")
        print("3. Stress Test")
        print("4. Exit")

        choice = input("Choice: ")

        if choice == "1":
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

        elif choice == "4":
            sock.close()
            break

        else:
            print("[!] Invalid choice")


if __name__ == "__main__":
    main()