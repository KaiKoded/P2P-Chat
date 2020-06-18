import socket
import threading

sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
sock.connect(("127.0.0.1", 12345))


class Listener(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.shutdown = False

    def run(self):
        while True:
            try:
                response = str(sock.recv(1024), "utf-8")
            except socket.error:
                sock.close()
                self.shutdown = True
                break
            print(response)


class Talker(threading.Thread):
    def __init__(self):
        threading.Thread.__init__(self)
        self.shutdown = False

    def run(self):
        while True:
            message = input("")
            if message == "" or message == "exit":
                sock.close()
                self.shutdown = True
                break
            data = bytes(message, "utf-8")
            try:
                sock.send(data)
            except socket.error:
                sock.close()
                self.shutdown = True
                break


listener = Listener()
listener.start()
talker = Talker()
talker.start()

print("Write something!")
