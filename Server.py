import socket
import threading


class Server(threading.Thread):
    def __init__(self, ip, port):
        threading.Thread.__init__(self)
        self.ip = ip
        self.port = port
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind((self.ip, self.port))
        self.sock.listen(1)
        print("listening to " + self.ip + ":" + str(self.port))

    def run(self):
        while True:
            conn, addr = self.sock.accept()
            print(addr[0] + ":" + str(addr[1]) + " connected.")
            cThread = threading.Thread(target=self.clientThread, args=(conn, addr))
            cThread.daemon = True
            cThread.start()

    def clientThread(self, conn, addr):
        while True:
            try:
                data = conn.recv(1024)
            except socket.error:
                print("client thread of " + addr[0] + ":" + str(addr[1]) + " encountered an error and is closing.")
                conn.close()
                break
            message = str(data, "utf-8")
            if message == "":
                print(addr[0] + ":" + str(addr[1]) + " disconnected.")
                conn.close()
                break
            print("The message is: " + message)

# Nur für test, später entfernen
if __name__ == "__main__":
    server = Server("0.0.0.0", 12345)
    server.start()
