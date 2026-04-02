import socket
import json
import time
import random
import threading

HEADER_SIZE = 10
FORMAT = "utf-8"

DEFAULT_TOPICS = ["sports", "tech", "finance", "weather"]


def connect(host="localhost", port=9000): #connects to publisher port 9000 (broker)
    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM) 
    sock.connect((host, port))
    print("Connected to broker")
    return sock


def send(sock, data, lock):
    msg = json.dumps(data).encode(FORMAT) # sends data dict to json then to bytes via encode to broker
    header = f"{len(msg):<{HEADER_SIZE}}".encode(FORMAT) #makes it so that the header is fixed size
    with lock:
        sock.sendall(header + msg) #only one thread will send msg


def create_topic(sock, topic, lock):
    send(sock, {"cmd": "CREATE", "topic": topic}, lock)


def publish(sock, topic, message, lock):
    send(sock, {
        "cmd": "PUBLISH",
        "topic": topic,
        "data": message,
        "timestamp": time.time()
    }, lock)


def random_news(topics):
    news_data = {
        "sports":  ["India won the match", "Messi scored a hattrick", "Olympics announced new events"],
        "tech":    ["New AI model released", "Quantum computing breakthrough", "New smartphone launched"],
        "finance": ["Stock market hits record high", "Crypto prices surge", "Interest rates increased"],
        "weather": ["Heavy rains expected tomorrow", "Heatwave alert issued", "Cyclone approaching coast"],
    }
    topic = random.choice(topics)
    msg = random.choice(news_data.get(topic, [f"Update on {topic}"]))
    return topic, msg


def auto_publish(sock, lock, topics, delay=2):
    print("[AUTO MODE] Streaming news... Ctrl+C to stop\n")
    try:
        while True:
            topic, msg = random_news(topics)
            publish(sock, topic, msg, lock)
            print(f"[AUTO] {topic} → {msg}")
            time.sleep(delay)
    except KeyboardInterrupt:
        print("\n[!] Auto mode stopped")


def stress_test(sock, lock, topics, n=1000):
    print(f"[STRESS TEST] Sending {n} messages...")
    start = time.time()
    threads = []
    for _ in range(n):
        topic, msg = random_news(topics)
        t = threading.Thread(target=publish, args=(sock, topic, msg, lock))
        t.start()
        threads.append(t)
    for t in threads:
        t.join() # waits until each thread is completed
    end = time.time()
    print(f"Time: {end - start:.4f}s")
    print(f"Throughput: {n / (end - start):.2f} msgs/sec")


def main():
    sock = connect()
    lock = threading.Lock() # lets say pub sends 2 msgs the msgs might get interleaved 

    active_topics = list(DEFAULT_TOPICS)
    for t in active_topics:
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
            print(f"Active topics: {active_topics}")
            topic = input("Enter topic name (existing or new): ").strip()
            if not topic:
                print("[ERROR] Topic name cannot be empty.")
                continue
            if topic not in active_topics: # if not in topic then create 
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
