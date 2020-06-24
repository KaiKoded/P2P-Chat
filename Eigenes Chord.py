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
        print("self id = %s" % self.ring_position)
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
        #f = open("/tmp/chord.log", "a+")
        #f.write(str(self.id()) + " : " + info + "\n")
        #f.close()
        print(str(self.id()) + " : " + info)

    def start_daemons(self):
        # start the daemons
        self.daemons['server'] = Daemon(self, 'server')
        self.daemons['check_predecessor'] = Daemon(self, 'check_predecessor')
        #self.daemons['fix_fingers'] = Daemon(self, 'fix_fingers')
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
        print("Successor " + self.successor[0] + ":" + self.successor[1] + " an Position " + self.successor[2] + " gefunden.")
        finger_positions = (self.ring_position + 2 ** np.arange(0, m)) % SIZE
        for finger in finger_positions:
            info = self.succ(finger).split("_")
            if not [info[0], info[1]] in self.fingers.values():
                print("Finger " + info[0] + ":" + info[1] + " an Position " + info[2] + " gefunden.")
                self.fingers[int(info[2])] = [info[0], info[1]]

        self.log("joined")

    def succ(self, k, entry = None):
        print("succ(): Looking for key " + str(k))

        if entry is not None:
            address_to_connect_to = entry
            #message = "JOIN_" + k + "_" + "ID" + "_" + self.ring_position
        else:
            distance_to_successor = (int(self.successor[2]) - int(self.ring_position)) % SIZE
            if distance_to_successor == 0:
                distance_to_successor = SIZE
            distance_to_key = (int(k) - self.ring_position) % SIZE
            print("Distanz zum Successor: " + str(distance_to_successor))
            print("Distanz zum Key: " + str(distance_to_key))
            if distance_to_key <= distance_to_successor:
                print("Returning: " + str(self.successor[0]) + "_" + str(self.successor[1]) + "_" + str(self.successor[2]))
                return str(self.successor[0]) + "_" + str(self.successor[1]) + "_" + str(self.successor[2])
            finger_positions = np.array(list(self.fingers.keys()))
            finger_distances = (int(k) - finger_positions) % SIZE
            id_of_closest_finger = int(finger_positions[np.where(finger_distances == np.min(finger_distances))])
            address_to_connect_to = tuple(self.fingers[id_of_closest_finger])
        message = "SUCC_" + str(k) + "_" + "LISTENING" + str(self.port) + "_" + "ID" + "_" + str(self.ring_position)
        self.succsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("succ(): Verbinde mit " + address_to_connect_to[0] + ":" + str(address_to_connect_to[1]))
        self.succsock.connect((address_to_connect_to[0],int(address_to_connect_to[1])))
        self.succsock.send(bytes(message, "utf-8"))
        response = str(self.succsock.recv(BUFFER_SIZE), "utf-8")
        print("Antwort erhalten: " + response)
        self.succsock.close()
        return response

    @repeat_and_sleep(CHECK_PREDECESSOR_INT)
    @retry_on_socket_error(CHECK_PREDECESSOR_RET)
    def check_predecessor(self):
        print("check_predecessor(): Pinging predecessor.")
        if self.predecessor == [self.ip, self.port, self.ring_position]:
            print("check_predecessor(): Ich bin mein eigener Predecessor.")
            return
        if self.predecessor == []:
            print("Kein predecessor vorhanden.")
            return
        self.cpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("check_predecessor(): Verbinde mit " + str(self.predecessor[0]) + ":" + str(self.predecessor[1]))
        self.cpsock.connect((self.predecessor[0], int(self.predecessor[1])))
        self.cpsock.send(bytes("PING", "utf-8"))
        response = str(self.cpsock.recv(BUFFER_SIZE), "utf-8")
        if not response:
            print("check_predecessor(): Keine Antwort erhalten. Entferne predecessor.")
            self.predecessor = []
            self.cpsock.close()
            return
        print("check_predecessor(): Antwort erhalten.")
        self.cpsock.close()

    @repeat_and_sleep(STABILIZE_INT)
    @retry_on_socket_error(STABILIZE_RET)
    def stabilize(self):
        if self.successor == [self.ip, self.port, self.ring_position]:
            print("stabilize(): Alles in Ordnung.")
            return
        self.stabsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("stabilize(): Verbinde mit " + str(self.successor[0]) + ":" + str(self.successor[1]))
        self.stabsock.connect((self.successor[0], int(self.successor[1])))
        self.stabsock.send(bytes("STABILIZE", "utf-8"))
        response = str(self.stabsock.recv(BUFFER_SIZE), "utf-8")
        print("stabilize(): Antwort erhalten: " + response)
        response = response.split("_")
        if int(response[2]) == self.ring_position:
            print("stabilize(): Alles in Ordnung.")
            return
        self.successor = response
        self.stabsock.close()
        self.stabsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        print("stabilize(): Verbinde mit " + str(self.successor[0]) + ":" + str(self.successor[1]))
        self.stabsock.connect((self.successor[0], int(self.successor[1])))
        print("stabilize(): Neuem Successor wird bescheid gesagt.")
        self.stabsock.send(bytes("PREDECESSOR?_" + str(self.ip) + "_" + str(self.port) + "_" + str(self.ring_position), "utf-8"))
        self.stabsock.close()

    def server(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", 12345))
        self.sock.listen(1)
        self.conns = {}
        self.threads = {}
        print("Listening to " + "0.0.0.0" + ":" + str(12345))
        while True:
            conn, addr = self.sock.accept()
            self.conns[addr[0] + ":" + str(addr[1])] = conn
            print(addr[0] + ":" + str(addr[1]) + " connected.")
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
                print(addr[0] + ":" + str(addr[1]) + " disconnected (Error).")
                break
            message = str(data, "utf-8")
            if not message:
                conn.close()
                del self.conns[addr[0] + ":" + str(addr[1])]
                del self.threads[addr[0] + ":" + str(addr[1])]
                print(addr[0] + ":" + str(addr[1]) + " disconnected (No Message).")
                break
            print("Message received: " + message)
            msgsplit = message.split("_")
            command = msgsplit[0]
            sending_peer_id = msgsplit[-1]
            response = ""
            if command == "SUCC":
                response = self.succ(msgsplit[1])
                if int(self.successor[2]) == int(self.ring_position):
                    print("Setze neuen successor, weil zuvor alleine im Netzwerk.")
                    self.successor = [addr[0], msgsplit[3], sending_peer_id]
            if command == "STABILIZE":
                response = str(self.predecessor[0]) + "_" + str(self.predecessor[1]) + "_" + str(self.predecessor[2])
            if command == "PREDECESSOR?":
                if self.predecessor == [] or int(self.predecessor[2]) == int(self.ring_position) or (int(sending_peer_id) - self.ring_position) % SIZE < (int(self.predecessor[2]) - self.ring_position) % SIZE:
                        print("Setze neuen Predecessor: " + str(addr[0]) + ":" + str(addr[1]))
                        self.predecessor = [addr[0], msgsplit[2], sending_peer_id]
            if command == "FIXFINGERS":
                pass
            if command == "PING":
                response = "PONG"

            if response != "":
                print("Sending response: " + response)
                self.conns[addr[0] + ":" + str(addr[1])].send(bytes(response, "utf-8"))
    # @repeat_and_sleep(STABILIZE_INT)
    # @retry_on_socket_error(STABILIZE_RET)
    # def stabilize(self):
    #     self.log("stabilize")
    #     suc = self.successor()
    #     # We may have found that x is our new successor iff
    #     # - x = pred(suc(n))
    #     # - x exists
    #     # - x is in range (n, suc(n))
    #     # - [n+1, suc(n)) is non-empty
    #     # fix finger_[0] if successor failed
    #     if suc.id() != self.finger_[0].id():
    #         self.finger_[0] = suc
    #     x = suc.predecessor()
    #     if x is not None and inrange(x.id(), self.id(1), suc.id()) and self.id(1) != suc.id() and x.ping():
    #         self.finger_[0] = x
    #     # We notify our new successor about us
    #     self.successor().notify(self)
    #     # Keep calling us
    #     return True
    #
    # def notify(self, remote):
    #     # Someone thinks they are our predecessor, they are iff
    #     # - we don't have a predecessor
    #     # OR
    #     # - the new node r is in the range (pred(n), n)
    #     # OR
    #     # - our previous predecessor is dead
    #     self.log("notify")
    #     if self.predecessor() == None or inrange(remote.id(), self.predecessor().id(1), self.id()) or not self.predecessor().ping():
    #         self.predecessor_ = remote

    # @repeat_and_sleep(FIX_FINGERS_INT)
    # def fix_fingers(self):
    #     # Randomly select an entry in finger_ table and update its value
    #     self.log("fix_fingers")
    #     i = random.randrange(m - 1) + 1
    #     self.finger_[i] = self.find_successor(self.id(1 << i))
    #     # Keep calling us
    #     return True
    #
    # @repeat_and_sleep(UPDATE_SUCCESSORS_INT)
    # @retry_on_socket_error(UPDATE_SUCCESSORS_RET)
    # def update_successors(self):
    #     self.log("update successor")
    #     suc = self.successor()
    #     # if we are not alone in the ring, calculate
    #     if suc.id() != self.id():
    #         successors = [suc]
    #         suc_list = suc.get_successors()
    #         if suc_list and len(suc_list):
    #             successors += suc_list
    #         # if everything worked, we update
    #         self.successors_ = successors
    #     return True
    #
    # def get_successors(self):
    #     self.log("get_successors")
    #     return map(lambda node: (node.address_.ip, node.address_.port), self.successors_[:N_SUCCESSORS - 1])
    #
    # def successor(self):
    #     # We make sure to return an existing successor, there `might`
    #     # be redundance between finger_[0] and successors_[0], but
    #     # it doesn't harm
    #     for remote in [self.finger_[0]] + self.successors_:
    #         if remote.ping():
    #             self.finger_[0] = remote
    #             return remote
    #     print("No successor available, aborting")
    #     self.shutdown_ = True
    #     sys.exit(-1)
    #
    # def predecessor(self):
    #     return self.predecessor_
    #
    # # @retry_on_socket_error(FIND_SUCCESSOR_RET)
    # def find_successor(self, id):
    #     # The successor of a key can be us iff
    #     # - we have a pred(n)
    #     # - id is in (pred(n), n]
    #     self.log("find_successor")
    #     if self.predecessor() and \
    #             inrange(id, self.predecessor().id(1), self.id(1)):
    #         return self
    #     node = self.find_predecessor(id)
    #     return node.successor()
    #
    # # @retry_on_socket_error(FIND_PREDECESSOR_RET)
    # def find_predecessor(self, id):
    #     self.log("find_predecessor")
    #     node = self
    #     # If we are alone in the ring, we are the pred(id)
    #     if node.successor().id() == node.id():
    #         return node
    #     while not inrange(id, node.id(1), node.successor().id(1)):
    #         node = node.closest_preceding_finger(id)
    #     return node
    #
    # def closest_preceding_finger(self, id):
    #     # first fingers in decreasing distance, then successors in
    #     # increasing distance.
    #     self.log("closest_preceding_finger")
    #     for remote in reversed(self.successors_ + self.finger_):
    #         if remote != None and inrange(remote.id(), self.id(1), id) and remote.ping():
    #             return remote
    #     return self



if __name__ == "__main__":
    local = LocalNode("192.168.178.20", 12345)
    #local.start()