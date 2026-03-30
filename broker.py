import socket
import threading
HOST='0.0.0.0'
PORT=5000
topics = {}
lock=threading.Lock()
def handle_client(conn, addr):
    print(f"[CONNECTED] {addr}")
    while True:
        try:
            msg=conn.recv(1024).decode()
            if not msg:
                break
            parts=msg.split(" ",2)
            command=parts[0]
            if command=="SUBSCRIBE":
                topic=parts[1]
                with lock:
                    if topic not in topics:
                        topics[topic]=[]
                    if conn not in topics[topic]:
                        topics[topic].append(conn)
                print(f"[SUBSCRIBE] {addr}->{topic}")
            elif command=="UNSUBSCRIBE":
                topic=parts[1]
                with lock:
                    if topic in topics and conn in topics[topic]:
                        topics[topic].remove(conn)
                print(f"[UNSUBSCRIBE] {addr}->{topic}")
            elif command=="PUBLISH":
                topic=parts[1]
                message=parts[2]
                print(f"[PUBLISH] {topic}:{message}")
                with lock:
                    subscribers=topics.get(topic,[])
                print(f"[ROUTING] sending to {len(subscribers)} subscribers")
                for sub in subscribers:
                    try:
                        sub.send(f"{topic}:{message}".encode())
                    except:
                        pass
        except:
            break
    with lock:
        for topic in topics:
            if conn in topics[topic]:
                topics[topic].remove(conn)
    conn.close()
    print(f"[DISCONNETED] {addr}")
def start_broker():
    server=socket.socket(socket.AF_INET,socket.SOCK_STREAM)
    server.setsockopt(socket.SOL_SOCKET,socket.SO_REUSEADDR,1)
    server.bind((HOST,PORT))
    server.listen()
    print(f"[BROKER RUNNING] {HOST}:{PORT}")
    while True:
        conn,addr=server.accept()
        thread=threading.Thread(target=handle_client,args=(conn,addr))
        thread.start()
if __name__=="__main__":
    start_broker()