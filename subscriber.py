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
    #this creates a tcp socket and connects to broker at localhost:9000
    sock.connect((HOST, PORT))
    print("[CONNECTED] Connected to broker")
    return sock

#the messages given to the function are encoded into a computer understandable format (bytes) 
#then it sends the message through the network (sendall)
def send_command(sock, command: str):
    sock.sendall(command.encode(FORMAT))

#creates a command string 'SUBSCRIBE sports' and sends it to user using send_command 
# the broker adds this client to the "sports subscriber list"
def subscribe(sock, topic: str):
    send_command(sock, f"SUBSCRIBE {topic}")
    print(f"[SUBSCRIBED] → {topic}")

#reates a command string 'UNSUBSCRIBE sports' and sends it to user using send_command 
# the broker then removes this client from the "sports subscriber list"
def unsubscribe(sock, topic: str):
    send_command(sock, f"UNSUBSCRIBE {topic}")
    print(f"[UNSUBSCRIBED] → {topic}")

def request_topic_list(sock):
    send_command(sock, "LIST_TOPICS")


def receive_messages(sock):
    #Background thread: continuously listen for incoming messages.
    global available_topics
    print("[LISTENING] Waiting for messages...\n")
    buffer = ""

    while True:
        try:
            # gets up to 4096 bytes from socket. messages are then changed to human readable format from byte.
            data = sock.recv(4096).decode(FORMAT)
            #if there is a break in the connection it is detected and exits the loop
            if not data:
                print("[DISCONNECTED] Broker closed the connection.")
                break

            #storing incoming data from socket
            buffer += data

           
            while "\n" in buffer: #until we find  newline from broker
                line, buffer = buffer.split("\n", 1) #extracts the first line from buffer
                line = line.strip()
                if not line:
                    continue

                if line.startswith("TOPICS:"):
                    raw = line[len("TOPICS:"):]#simply removes the word "TOPICS"
                    new_topics = [t for t in raw.split(",") if t] #splits the remining topics
                    with topics_lock:
                        available_topics = new_topics #update the topics 
                    print(f"\n[TOPICS UPDATED] {available_topics}")
                    print("Choice: ", end="", flush=True)

                elif ":" in line:
                     #splits the info into topic, message. uses only the first colon
                    topic, message = line.split(":", 1)
                    timestamp = time.strftime("%H:%M:%S")
                    # formatting the notification that the broker sends
                    content = f"📨 [{topic.upper()}] {message}"
                    width = len(content) + 4

                    print("\n" + "═" * width)
                    print(f"║ {content} ║")
                    print(f"║ ⏰ {timestamp}{' ' * (width - len(timestamp) - 5)}║")
                    print("═" * width + "\n")
                    #cursor stays in the same line after printing and 
                    # When a message arrives and prints, flush=True ensures the input prompt is immediately shown again without delay.
                    print("Choice: ", end="", flush=True) # Re-prompt

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

         #if anything goes wrong in the try block it goes here and print error and stop the loop
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
    #this is a list to track user subscriptions it prevents duplicate subscriptions
    subscribed_topics = []

    # Fetch initial topic list from broker
    request_topic_list(sock)

    # this runs receive_messages() in the background which helps with receiving of messages without blocking the user input
    #daemon = true ; Thread automatically exits when main program exits
    listener = threading.Thread(target=receive_messages, args=(sock,), daemon=True)
    listener.start()

    # Give the listener a moment to receive the initial topic list
    time.sleep(0.3)

    #this is the menu loop where the user interacts by using subscribe unsubscribe etc
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
            topic = input("Enter topic to subscribe : ").strip()
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
            #loops through every topic that has been subscribed to and unsubscribes from them
            for topic in subscribed_topics:
                unsubscribe(sock, topic)
            sock.close()
            break

        else:
            print("[!] Invalid choice. Try again.")


if __name__ == "__main__":
    main()