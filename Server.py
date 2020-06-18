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
        self.threads = []
        print("listening to " + self.ip + ":" + str(self.port))

    def run(self):
        while True:
            conn, addr = self.sock.accept()
            self.threads.append(conn)
            print(addr[0] + ":" + str(addr[1]) + " connected.")
            for connection in self.threads:
                connection.send(bytes(addr[0] + ":" + str(addr[1]) + " connected.", "utf-8"))
            cthread = threading.Thread(target=self.client_thread, args=(conn, addr))
            cthread.daemon = True
            cthread.start()

    def client_thread(self, conn, addr):
        while True:
            try:
                data = conn.recv(1024)
            except socket.error:
                conn.close()
                self.threads.remove(conn)
                print(addr[0] + ":" + str(addr[1]) + " disconnected.")
                for connection in self.threads:
                    connection.send(bytes(addr[0] + ":" + str(addr[1]) + " disconnected.", "utf-8"))
                break
            message = str(data, "utf-8")
            if message == "":
                conn.close()
                self.threads.remove(conn)
                print(addr[0] + ":" + str(addr[1]) + " disconnected.")
                for connection in self.threads:
                    connection.send(bytes(addr[0] + ":" + str(addr[1]) + " disconnected.", "utf-8"))
                break
            print(addr[0] + ":" + str(addr[1]) + " writes: " + message)
            for connection in self.threads:
                connection.send(bytes(addr[0] + ":" + str(addr[1]) + " writes: " + message, "utf-8"))


# Nur für test, später entfernen
if __name__ == "__main__":
    server = Server("0.0.0.0", 12345)
    server.start()
