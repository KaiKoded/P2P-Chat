import threading
import json
import random
import sys
import socket
import numpy as np

from HelperFunctions import *
from Settings import *

class Daemon(threading.Thread):
    def __init__(self, obj, method):
        threading.Thread.__init__(self)
        self.obj_ = obj
        self.method_ = method

    def run(self):
        getattr(self.obj_, self.method_)()

class LocalNode(object):
    def __init__(self, ip, port):
        # Eigene Adresse ist [IP, Port, Position]
        self.ip = ip
        self.port = port
        self.shutdown = False
        self.daemons = {}
        # Predecessor und Successor sind [IP, Port, Position]
        self.predecessor = []
        self.successor = []
        # Fingers sind [Position : [ID, Port]]
        self.fingers = {}
        #self.public_key = ?
        #self.private_key = ?
        self.entry_address = input("Please specify IP and Port of DHT entry (if empty, new DHT will be created): ")
        if self.entry_address != "":
            self.entry_address = self.entry_address.split(":")
            self.entry_address = (self.entry_address[0], int(self.entry_address[1]))
        self.username = input("Please choose your username: ")
        self.ring_position = self.id()
        print("Eigene Adresse = %s:%s" % (self.ip, self.port))
        print("Eigene ID = %s" % self.ring_position)
        self.join()
        self.start_daemons()
        #self.distribute(self.username)

    def shutdown(self):
        self.shutdown = True
        for connection in self.conns:
            connection.close()
        self.sock.shutdown(socket.SHUT_RDWR)
        self.sock.close()

    def log(self, info):
        print(str(self.id()) + " : " + info)

    def start_daemons(self):
        self.daemons['server'] = Daemon(self, 'server')
        self.daemons['check_predecessor'] = Daemon(self, 'check_predecessor')
        self.daemons['fix_fingers'] = Daemon(self, 'fix_fingers')
        self.daemons['stabilize'] = Daemon(self, 'stabilize')
        for key in self.daemons:
            self.daemons[key].start()
            print("Daemon " + key + " started.")
        self.log("started")

    def id(self):
        return (self.username.__hash__()) % SIZE

    def join(self):
        print("Join started.")
        if not self.entry_address:
            self.successor = [self.ip, self.port, self.ring_position]
            self.predecessor = [self.ip, self.port, self.ring_position]
            print("Neue Chord DHT wurde gestartet. Warte auf Verbindungen.")
            return
        self.successor = self.succ(self.ring_position, self.entry_address).split("_")
        self.notify_successor()
        print("Successor " + self.successor[0] + ":" + self.successor[1] + " an Position " + self.successor[2] + " gefunden.")
        finger_positions = (self.ring_position + 2 ** np.arange(0, m)) % SIZE
        print("Looking for fingers")
        self.fix_fingers()
        # for finger in finger_positions:
        #     info = self.succ(finger).split("_")
        #     if not [info[0], info[1]] in self.fingers.values():
        #         print("Finger " + info[0] + ":" + info[1] + " an Position " + info[2] + " gefunden.")
        #         self.fingers[int(info[2])] = [info[0], info[1]]
        self.log("joined")

    def succ(self, k, entry = None):
        #print("succ(): Looking for key " + str(k))

        if entry is not None:
            address_to_connect_to = entry
            #message = "JOIN_" + k + "_" + "ID" + "_" + self.ring_position
        else:
            distance_to_successor = (int(self.successor[2]) - int(self.ring_position)) % SIZE
            if distance_to_successor == 0:
                distance_to_successor = SIZE
            distance_to_key = (int(k) - self.ring_position) % SIZE
            #("Distanz zum Successor: " + str(distance_to_successor))
            #print("Distanz zum Key: " + str(distance_to_key))
            if distance_to_key <= distance_to_successor:
                #print("Returning: " + str(self.successor[0]) + "_" + str(self.successor[1]) + "_" + str(self.successor[2]))
                return str(self.successor[0]) + "_" + str(self.successor[1]) + "_" + str(self.successor[2])
            finger_positions = np.array(list(self.fingers))
            finger_distances = (int(k) - finger_positions) % SIZE
            id_of_closest_finger = int(finger_positions[np.where(finger_distances == np.min(finger_distances))])
            address_to_connect_to = tuple(self.fingers[id_of_closest_finger])
        message = "SUCC_" + str(k) + "_" + "LISTENING" + "_" + str(self.ip) + "_" + str(self.port) + "_" + "ID" + "_" + str(self.ring_position)
        self.succsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #print("succ(): Verbinde mit " + address_to_connect_to[0] + ":" + str(address_to_connect_to[1]))
        self.succsock.connect((address_to_connect_to[0],int(address_to_connect_to[1])))
        self.succsock.send(bytes(message, "utf-8"))
        response = str(self.succsock.recv(BUFFER_SIZE), "utf-8")
        #print("Antwort erhalten: " + response)
        self.succsock.close()
        return response

    def notify_successor(self):
        notisock = socket.socket(socket.AF_INET,socket.SOCK_STREAM)
        notisock.connect((self.successor[0], int(self.successor[1])))
        notisock.send(bytes("JOINED_LISTENING_" + self.ip + "_" + str(self.port) + "_" + str(self.ring_position),"utf-8"))
        notisock.settimeout(GLOBAL_TIMEOUT)
        try:
            self.predecessor = str(notisock.recv(BUFFER_SIZE),"utf-8").split("_")
        except socket.timeout:
            pass

    @repeat_and_sleep(CHECK_PREDECESSOR_INT)
    @retry_on_socket_error(CHECK_PREDECESSOR_RET)
    def check_predecessor(self):
        #print("check_predecessor(): Pinging predecessor.")
        if self.predecessor == [self.ip, self.port, self.ring_position]:
            #print("check_predecessor(): Ich bin mein eigener Predecessor.")
            return
        if self.predecessor == []:
            #print("Kein predecessor vorhanden.")
            return
        self.cpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.cpsock.settimeout(GLOBAL_TIMEOUT)
        #print("check_predecessor(): Verbinde mit " + str(self.predecessor[0]) + ":" + str(self.predecessor[1]))
        try:
            self.cpsock.connect((self.predecessor[0], int(self.predecessor[1])))
            self.cpsock.send(bytes("PING", "utf-8"))
            response = str(self.cpsock.recv(BUFFER_SIZE), "utf-8")
        except socket.timeout:
            print("check_predecessor(): Keine Antwort erhalten. Entferne predecessor.")
            self.predecessor = []
            self.cpsock.close()
            return
        #print("check_predecessor(): Antwort erhalten.")
        self.cpsock.close()

    @repeat_and_sleep(STABILIZE_INT)
    @retry_on_socket_error(STABILIZE_RET)
    def stabilize(self):
        if self.successor == [self.ip, self.port, self.ring_position]:
            #print("stabilize(): Alles in Ordnung.")
            return
        self.stabsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.stabsock.connect((self.successor[0], int(self.successor[1])))
        message = "STABILIZE_LISTENING_" + str(self.ip) + "_" + str(self.port) + "_ID_" + str(self.ring_position)
        self.stabsock.send(bytes(message, "utf-8"))
        #print("stabilize(): Nachricht an " + str(self.successor[0]) + ":" + str(self.successor[1]) + " : " + message)
        response = str(self.stabsock.recv(BUFFER_SIZE), "utf-8").split("_")
        #print("stabilize(): Antwort erhalten: " + "_".join(response))
        if int(response[2]) == self.ring_position:
            #print("stabilize(): Alles in Ordnung.")
            return
        print("stabilize(): Setze neuen successor: " + response[0] + ":" + response[1] + " (" + response[2] + ")")
        self.successor = response
        self.stabsock.close()
        self.stabsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        #print("stabilize(): Verbinde mit " + str(self.successor[0]) + ":" + str(self.successor[1]))
        self.stabsock.connect((self.successor[0], int(self.successor[1])))
        #print("stabilize(): Neuem Successor wird bescheid gesagt.")
        self.stabsock.send(bytes("PREDECESSOR?_" + str(self.ip) + "_" + str(self.port) + "_" + str(self.ring_position), "utf-8"))
        self.stabsock.close()

    @repeat_and_sleep(FIX_FINGERS_INT)
    def fix_fingers(self):
        finger_positions = (self.ring_position + 2 ** np.arange(0, m)) % SIZE
        for finger in list(self.fingers):
            try:
                self.ffsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
                self.ffsock.settimeout(GLOBAL_TIMEOUT)
                self.ffsock.connect((self.fingers[finger][0], int(self.fingers[finger][1])))
                self.ffsock.send(bytes("PING", "utf-8"))
                response = str(self.ffsock.recv(BUFFER_SIZE), "utf-8")
            except socket.timeout:
                print("fix_fingers(): Keine Antwort erhalten. Entferne finger " + str(self.fingers[finger][0]) + ":" + str(self.fingers[finger][1]) + " (" + str(finger) + ")")
                del self.fingers[finger]
                self.ffsock.close()
        for finger in finger_positions:
            info = self.succ(finger).split("_")
            if not [info[0], info[1]] in self.fingers.values():
                print("fix_fingers(): Finger " + info[0] + ":" + info[1] + " an Position " + info[2] + " gefunden.")
                self.fingers[int(info[2])] = [info[0], info[1]]

    def server(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.listen(10)
        self.conns = {}
        self.threads = {}
        print("Listening to " + "0.0.0.0" + ":" + str(self.port))
        while True:
            conn, addr = self.sock.accept()
            self.conns[addr[0] + ":" + str(addr[1])] = conn
            #print(addr[0] + ":" + str(addr[1]) + " connected.")
            cthread = threading.Thread(target=self.client_thread, args=(conn, addr))
            cthread.daemon = True
            cthread.start()
            self.threads[addr[0] + ":" + str(addr[1])] = cthread

    def client_thread(self, conn, addr):
        while True:
            try:
                data = conn.recv(BUFFER_SIZE)
            except socket.error:
                conn.close()
                del self.conns[addr[0] + ":" + str(addr[1])]
                del self.threads[addr[0] + ":" + str(addr[1])]
                #print(addr[0] + ":" + str(addr[1]) + " disconnected (Error).")
                break
            message = str(data, "utf-8")
            if not message:
                conn.close()
                del self.conns[addr[0] + ":" + str(addr[1])]
                del self.threads[addr[0] + ":" + str(addr[1])]
                #print(addr[0] + ":" + str(addr[1]) + " disconnected (No Message).")
                break
            #print("Message received: " + message)
            msgsplit = message.split("_")
            command = msgsplit[0]
            sending_peer_id = msgsplit[-1]
            response = ""
            if command == "SUCC":
                response = self.succ(msgsplit[1])
                if int(self.successor[2]) == int(self.ring_position):
                    print("Server: Setze neuen successor, weil zuvor alleine im Netzwerk: " + msgsplit[3] + ":" + msgsplit[4] + " (" + str(sending_peer_id) + ")")
                    self.successor = [msgsplit[3], msgsplit[4], sending_peer_id]
                    self.fingers[int(sending_peer_id)] = [msgsplit[3], msgsplit[4]]
            if command == "STABILIZE":
                if self.predecessor == []:
                    print("Server: Setze neuen Predecessor da zuvor keiner vorhanden: " + msgsplit[2] + ":" + msgsplit[3] + " (" + str(sending_peer_id) + ")")
                    self.predecessor = [msgsplit[2], msgsplit[3], sending_peer_id]
                response = str(self.predecessor[0]) + "_" + str(self.predecessor[1]) + "_" + str(self.predecessor[2])
            if command == "PREDECESSOR?":
                if self.predecessor == [] or int(self.predecessor[2]) == int(self.ring_position) or (self.ring_position - int(sending_peer_id)) % SIZE < (self.ring_position - int(self.predecessor[2])) % SIZE:
                        print("Server: Setze neuen Predecessor: " + msgsplit[1] + ":" + msgsplit[2] + " (" + str(sending_peer_id) + ")")
                        self.predecessor = [msgsplit[1], msgsplit[2], sending_peer_id]
            if command == "FIXFINGERS":
                pass
            if command == "PING":
                response = "PONG"
            if command == "JOINED":
                print("join(): Setze neuen Predecessor: " + msgsplit[2] + ":" + msgsplit[3] + " (" + msgsplit[4] + ")")
                self.predecessor = [msgsplit[2], msgsplit[3], msgsplit[4]]
                response = str(self.predecessor[0]) + "_" + str(self.predecessor[1]) + "_" + str(self.predecessor[2])

            if response != "":
                #print("Sending response: " + response)
                self.conns[addr[0] + ":" + str(addr[1])].send(bytes(response, "utf-8"))


if __name__ == "__main__":
    local = LocalNode("192.168.178.20", 12345)