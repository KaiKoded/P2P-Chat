import threading
import json
import random
import sys
import socket
import numpy as np
import hashlib
from datetime import datetime, timedelta


from HelperFunctions import *
from Settings import *
from KeyGen import *

class Daemon(threading.Thread):
    def __init__(self, obj, method):
        threading.Thread.__init__(self)
        self.obj_ = obj
        self.method_ = method

    def run(self):
        getattr(self.obj_, self.method_)()


class LocalNode(object):
    def __init__(self, app, port: int, entry_address: str, username: str):
        self.app = app

        # Eigene Adresse ist [IP, Port, Position]
        self.port = port
        self.shutdown_ = False
        self.daemons = {}
        # Predecessor und Successor sind [IP, Port, Position]
        self.predecessor = []
        self.successor = []
        # Fingers sind [Position : [ID, Port]]
        self.fingers = {}
        # DHT Einträge sind {hash : [IP, Port, Public Key, Timestamp]}
        self.keys = {}
        self.lock = threading.Lock()
        self.entry_address = entry_address
        self.private_key = getPrivateKey()
        self.public_key = str(serializePublicKey(getPublicKey(self.private_key)), "utf-8")
        if self.entry_address != "":
            self.entry_address = self.entry_address.split(":")
            self.entry_address = (":".join(self.entry_address[:-1]), int(self.entry_address[-1]))
        self.username = username
        self.ring_position = self.id()
        print(f"Eigener Port = {self.port}")
        print(f"Eigene Ringposition = {self.ring_position}")
        self.join()
        self.start_daemons()

    def hash_username(self, username: str):
        return int(hashlib.sha1(username.encode("utf-8")).hexdigest(), 16) % SIZE

    def shutdown(self):
        self.shutdown_ = True
        print("shutdown() : Initiating shutdown.")
        try:
            to_successor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            to_successor.settimeout(GLOBAL_TIMEOUT)
            to_successor.connect((self.successor[0], self.successor[1]))
            to_successor.send(bytes("SHUTDOWN_PREDECESSOR:_" + self.predecessor[0] + "_" + str(self.predecessor[1]) + "_" + str(self.predecessor[2]), "utf-8"))
            to_successor.close()
            to_predecessor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            to_predecessor.settimeout(GLOBAL_TIMEOUT)
            to_predecessor.connect((self.predecessor[0], self.predecessor[1]))
            to_predecessor.send(bytes("SHUTDOWN_SUCCESSOR:_" + self.successor[0] + "_" + str(self.successor[1]) + "_" + str(self.successor[2]), "utf-8"))
            to_predecessor.close()
        except socket.error:
            print("shutdown() : Fehler beim Senden der Successor- oder Predecessor-Informationen.")
        self.give_keys((self.successor[0], self.successor[1]), self.ring_position)
        for connection in list(self.conns):
            self.conns[connection].close()
        sys.exit(0)

    def start_daemons(self):
        self.daemons['server'] = Daemon(self, 'server')
        self.daemons['check_predecessor'] = Daemon(self, 'check_predecessor')
        self.daemons['fix_fingers'] = Daemon(self, 'fix_fingers')
        self.daemons['stabilize'] = Daemon(self, 'stabilize')
        self.daemons['check_distributed_name'] = Daemon(self, 'check_distributed_name')
        for key in self.daemons:
            self.daemons[key].start()
        print("Daemons started.")

    def id(self):
        # return hash(self.username) % SIZE
        # return int(hashlib.sha1(self.username.encode("utf-8")).hexdigest(), 16) % SIZE
        return random.randint(0, SIZE - 1)

    def join(self):
        print("Join started.")
        if not self.entry_address:
            self.successor = ["", self.port, self.ring_position]
            self.predecessor = ["", self.port, self.ring_position]
            username_key = int(hashlib.sha1(self.username.encode("utf-8")).hexdigest(), 16) % SIZE
            print("join() : Speichere eigenen username mit position " + str(username_key) + " bei sich selbst.")
            self.keys[username_key] = ["", self.port, self.public_key, datetime.timestamp(datetime.utcnow())]
            print("Neue Chord DHT wurde gestartet. Warte auf Verbindungen.")
            return
        print("join () : Suche successor von eigener Ringposition")
        succinfo = self.succ(self.ring_position, self.entry_address).split("_")
        if succinfo[0] == "ERROR":
            print("Entry-Adresse nicht erreichbar. Programm wird beendet.")
            self.shutdown()
            sys.exit(0)
        if not succinfo[0]:
            succinfo[0] = self.entry_address[0]
        self.successor = [succinfo[0], int(succinfo[1]), int(succinfo[2])]
        print("join () : Gebe successor bescheid")
        self.notify_successor()
        self.fingers[int(self.successor[2])] = [self.successor[0], self.successor[1]]
        print("Successor " + self.successor[0] + ":" + str(self.successor[1]) + " an Position " + str(
            self.successor[2]) + " gefunden.")
        finger_positions = (self.ring_position + 2 ** np.arange(0, m)) % SIZE
        print("Looking for fingers")
        print("Finger " + self.successor[0] + ":" + str(self.successor[1]) + " an Position " + str(
            self.successor[2]) + " gefunden.")
        found = self.successor[2]
        for finger in finger_positions:
            if (finger - self.ring_position) % SIZE > (found - self.ring_position) % SIZE:
                # if not (self.successor[2] - finger) % SIZE < (self.successor[2] - self.ring_position) % SIZE:
                print("join() : Suche nach Finger mit Position " + str(finger))
                info = self.succ(finger).split("_")
                if info[0] == "ERROR":
                    continue
                found = int(info[2])
                if not int(info[2]) == self.ring_position:
                    print("Finger " + info[0] + ":" + info[1] + " an Position " + info[2] + " gefunden.")
                    self.fingers[int(info[2])] = [info[0], int(info[1])]
        distribute_status = self.distribute_name()
        if distribute_status == "ERROR":
            sys.exit(0)
        print("Join finished.")

    # @retry_on_socket_error(SUCC_RET)
    def succ(self, k, entry=None):
        # print("succ() : Trying to acquire lock.")
        # print("succ() : Lock acquired.")
        retries = 0
        while retries < SUCC_RET:
            self.lock.acquire()
            if entry is not None:
                address_to_connect_to = entry
                # message = "JOIN_" + k + "_" + "ID" + "_" + self.ring_position
            else:
                distance_to_successor = (self.successor[2] - self.ring_position) % SIZE
                if distance_to_successor == 0:
                    distance_to_successor = SIZE
                distance_to_key = (k - self.ring_position) % SIZE
                #print("Distanz zum Successor: " + str(distance_to_successor))
                #print("Distanz zum Key: " + str(distance_to_key))
                if distance_to_key <= distance_to_successor:
                    #print("succ() : Returning: " + str(self.successor[0]) + "_" + str(self.successor[1]) + "_" + str(self.successor[2]))
                    self.lock.release()
                    return self.successor[0] + "_" + str(self.successor[1]) + "_" + str(self.successor[2])
                finger_positions = np.array(list(self.fingers))
                finger_distances = (k - finger_positions) % SIZE
                try:
                    id_of_closest_finger = int(finger_positions[np.where(finger_distances == np.min(finger_distances))])
                    if np.min(finger_distances) > (k - self.successor[2]) % SIZE:
                        # print("succ() : Route über successor " + str(self.successor[2]))
                        address_to_connect_to = (self.successor[0], self.successor[1])
                    else:
                        address_to_connect_to = tuple(self.fingers[id_of_closest_finger])
                        # print("succ() : Route über Finger " + str(id_of_closest_finger))
                except ValueError:
                    address_to_connect_to = (self.successor[0], self.successor[1])
                # print("succ() : Suche nach " + str(k))
            # print("succ(): Looking for key " + str(k) + " at " + str(address_to_connect_to))
            message = "SUCC_" + str(k) + "_" + "LISTENING_" + str(self.port) + "_" + "ID" + "_" + str(self.ring_position)
            self.succsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.succsock.settimeout(GLOBAL_TIMEOUT)
            # print("succ(): Verbinde mit " + address_to_connect_to[0] + ":" + str(address_to_connect_to[1]))
            # print("succ(): Mein aktueller successor: " + str(self.successor))
            # print("succ(): Gesucht wird bei: " + address_to_connect_to[0] + ":" + str(address_to_connect_to[1]))
            try:
                self.succsock.connect(address_to_connect_to)
                self.succsock.send(bytes(message, "utf-8"))
                # print("succ(): Warte auf Antwort von " + str(address_to_connect_to[0] + ":" + str(address_to_connect_to[1])))
                # print("Mein Successor: " + str(self.successor))
                # print("Meine Finger:")
                # print(list(self.fingers.values()))
                response = str(self.succsock.recv(BUFFER_SIZE), "utf-8").split("_")
                if response[0] == "":
                    response[0] = address_to_connect_to[0]
                response = "_".join(response)
                # print("succ(): Antwort erhalten.")
                self.succsock.close()
                self.lock.release()
                # print("succ() : Abgeschlossen. Lock released.")
                return response
            except socket.error:
                self.lock.release()
                print(str(threading.currentThread()) + " : succ() : Socket error. Retrying...")
                retries += 1
                time.sleep(3)
            if retries == SUCC_RET:
                print("succ() : Request failed!")
                return "ERROR"

    def notify_successor(self):
        notisock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        notisock.connect((self.successor[0], self.successor[1]))
        notisock.send(
            bytes("JOINED_LISTENING_" + str(self.port) + "_" + str(self.ring_position), "utf-8"))
        notisock.settimeout(GLOBAL_TIMEOUT)
        # try:
        # predinfo = str(notisock.recv(BUFFER_SIZE), "utf-8").split("_")
        # self.predecessor = [predinfo[0], int(predinfo[1]), int(predinfo[2])]
        # except socket.timeout:
        # pass
        notisock.close()

    @repeat_and_sleep(CHECK_PREDECESSOR_INT)
    @retry_on_socket_error(CHECK_PREDECESSOR_RET)
    def check_predecessor(self):
        # print("check_predecessor(): Pinging predecessor.")
        if not self.predecessor:
            # print("Kein predecessor vorhanden.")
            return
        if self.predecessor[2] == self.ring_position:
            # print("check_predecessor(): Ich bin mein eigener Predecessor.")
            return
        self.cpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.cpsock.settimeout(GLOBAL_TIMEOUT)
        # print("check_predecessor(): Pinge " + str(self.predecessor[0]) + ":" + str(self.predecessor[1]))
        try:
            self.cpsock.connect((self.predecessor[0], self.predecessor[1]))
            self.cpsock.send(bytes("PING", "utf-8"))
            response = str(self.cpsock.recv(BUFFER_SIZE), "utf-8")
        except socket.error:
            print("check_predecessor(): Keine Antwort erhalten. Entferne predecessor " + str(self.predecessor[2]))
            self.predecessor = []
            self.cpsock.close()
            return
        # print("check_predecessor(): Antwort erhalten.")
        self.cpsock.close()

    @repeat_and_sleep(STABILIZE_INT)
    @retry_on_socket_error(STABILIZE_RET)
    def stabilize(self):
        # print("stabilize() : Started")
        if self.successor[2] == self.ring_position:
            # print("stabilize(): Alles in Ordnung.")
            return
        self.stabsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.stabsock.settimeout(GLOBAL_TIMEOUT)
        try:
            self.stabsock.connect((self.successor[0], self.successor[1]))
        except socket.error:
            print("stabilize() : Successor " + str(self.successor[2]) + " antwortet nicht. Setze neuen Successor.")
            self.newsuccessor()
            self.stabsock.close()
            return
        message = "STABILIZE_LISTENING_" + str(self.port) + "_ID_" + str(self.ring_position)
        self.stabsock.send(bytes(message, "utf-8"))
        # print("stabilize(): Nachricht an " + str(self.successor[0]) + ":" + str(self.successor[1]) + " : " + message)
        response = str(self.stabsock.recv(BUFFER_SIZE), "utf-8").split("_")
        # print("stabilize(): Antwort erhalten: " + "_".join(response))
        if int(response[2]) == self.ring_position:
            # print("stabilize(): Alles in Ordnung.")
            return
        # print("stabilize(): " + response[2] + " unterscheidet sich von " + str(self.ring_position))
        print("stabilize(): Setze neuen successor: " + response[0] + ":" + response[1] + " (" + response[2] + ")")
        self.successor = [response[0], int(response[1]), int(response[2])]
        self.stabsock.close()
        self.stabsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        # print("stabilize(): Verbinde mit " + str(self.successor[0]) + ":" + str(self.successor[1]))
        self.stabsock.connect((self.successor[0], self.successor[1]))
        # print("stabilize(): Neuem Successor wird bescheid gesagt.")
        self.stabsock.send(
            bytes("PREDECESSOR?_" + str(self.port) + "_" + str(self.ring_position), "utf-8"))
        self.stabsock.close()

    def newsuccessor(self):
        fingers_without_successor = [x for x in list(self.fingers) if x != self.successor[2]]
        if not fingers_without_successor:
            print(
                "Keine Fingereinträge übrig! Setze sich selbst als Successor, um neu aufgesetztes Chord zu simulieren.")
            self.successor = ["", self.port, self.ring_position]
            return
        finger_positions = np.array(fingers_without_successor)
        potential_new_successors = (self.ring_position - finger_positions) % SIZE
        new_successor = int(finger_positions[np.where(potential_new_successors == np.min(potential_new_successors))])
        self.successor = self.fingers[new_successor] + [new_successor]
        print("stabilize() : Neuer Successor: " + str(new_successor))

    @repeat_and_sleep(FIX_FINGERS_INT)
    @retry_on_socket_error(FIX_FINGERS_RET)
    def fix_fingers(self):
        if self.successor[2] == self.ring_position:
            return
        finger_positions = (self.ring_position + 2 ** np.arange(0, m)) % SIZE
        # finger_diff = False
        found = None
        for fpos in finger_positions:
            if not found or (fpos - self.ring_position) % SIZE > (found - self.ring_position) % SIZE:
                info = self.succ(fpos).split("_")
                if info[0] == "ERROR":
                    continue
                if not [info[0], int(info[1])] in self.fingers.values() and not int(info[2]) == self.ring_position:
                    print("fix_fingers(): Finger " + info[0] + ":" + info[1] + " an Position " + info[2] + " gefunden.")
                    self.fingers[int(info[2])] = [info[0], int(info[1])]
                    found = int(info[2])
        if self.fingers:
            times_minimum = np.zeros_like(list(self.fingers))
            for fpos in finger_positions:
                distances = (np.array(list(self.fingers)) - fpos) % SIZE
                times_minimum[np.where(distances == np.min(distances))] += 1
                to_delete = np.where(times_minimum == 0)[0].tolist()
            if to_delete:
                for d in to_delete:
                    print("fix_fingers(): Lösche redundanten Finger " + self.fingers[list(self.fingers.keys())[d]][
                        0] + ":" + str(self.fingers[list(self.fingers.keys())[d]][1]) + " (" + str(
                        list(self.fingers.keys())[d]) + ")")
                    del self.fingers[list(self.fingers.keys())[d]]
        # print("fix_fingers(): Pinging Finger list.")
        for finger in list(self.fingers):
            self.ffsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.ffsock.settimeout(GLOBAL_TIMEOUT)
            try:
                # print("fix_fingers(): Pinging " + self.fingers[finger][0] + ":" + str(self.fingers[finger][1]))
                self.ffsock.connect((self.fingers[finger][0], self.fingers[finger][1]))
                self.ffsock.send(bytes("PING", "utf-8"))
                response = str(self.ffsock.recv(BUFFER_SIZE), "utf-8")
                # print("fix_fingers(): Response received.")
            except socket.error:
                print(
                    "fix_fingers(): Keine Antwort erhalten. Entferne finger " + self.fingers[finger][0] + ":" + str(
                        self.fingers[finger][1]) + " (" + str(finger) + ")")
                del self.fingers[finger]
            self.ffsock.close()
        # print("fix_fingers(): Looking for new fingers.")

    def distribute_name(self):
        username_key = self.hash_username(self.username)
        print("distribute_name() : Username-Ringposition: " + str(username_key))
        responsible_peer = self.succ(username_key).split("_")
        print("distribute_name() : Verantwortlicher Peer: " + responsible_peer[0] + ":" + str(responsible_peer[1]) + " (" + str(responsible_peer[2]) + ")")
        if responsible_peer == "ERROR":
            print("distribute_name() : Name konnte nicht verteilt werden, da verantwortlicher Peer nicht erreichbar ist."
                  "Join wird abgebrochen.")
            return "ERROR"
        # Port und Ringposition sind Integers:
        if int(responsible_peer[2]) == self.ring_position or (int(responsible_peer[2]) - username_key) % SIZE > (self.ring_position - username_key) % SIZE:
            self.keys[username_key] = ["", self.port, self.public_key, datetime.timestamp(datetime.utcnow())]
            print("distribute_name() : Key wird bei sich selbst gespeichert.")
            return
        distsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            distsock.connect((responsible_peer[0], int(responsible_peer[1])))
            distsock.settimeout(GLOBAL_TIMEOUT)
            message = "DISTRIBUTE_" + str(username_key) + "_LISTENING_" + str(self.port) + "_PUBLICKEY_" + self.public_key + "_" + str(self.ring_position)
            #print("distribute_name() : Sende Nachricht " + message)
            distsock.send(bytes(message, "utf-8"))
            response = distsock.recv(BUFFER_SIZE).split(bytes("_", "utf-8"))
            if str(response[0], "utf-8") == "SUCCESS":
                return "SUCCESS"
            if str(response[0], "utf-8") == "DECRYPT":
                encrypted_message = bytes("_", "utf-8").join(response[1:])
                decrypted_message = decrypt(self.private_key, encrypted_message)
                distsock.send(decrypted_message)
                response = str(distsock.recv(BUFFER_SIZE), "utf-8")
                if response == "SUCCESS":
                    return "SUCCESS"
                if response == "FAILURE":
                    return "ERROR"
        except socket.error:
            print("distribute_name(): Socket Error.")
            return "ERROR"

    @repeat_and_sleep(CHECK_DISTRIBUTE_INT)
    def check_distributed_name(self):
        username_key = self.hash_username(self.username)
        if username_key in list(self.keys):
            #print("check_distributed_name() : Key ist bei sich selbst gespeichert.")
            return
        responsible_peer = self.succ(username_key).split("_")
        if responsible_peer[0] == "ERROR":
            print("check_distributed_name() : Verantwortlicher Peer ist offenbar ausgefallen. Name wird bei der nächsten Iteration neu gesetzt.")
            return
        # Port und Ringposition sind Integers:
        if (int(responsible_peer[2]) - username_key) % SIZE > (self.ring_position - username_key) % SIZE:
            print("check_distributed_name() : Key ist beim falschen Peer gespeichert!!!")
            return
        distsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            distsock.connect((responsible_peer[0], int(responsible_peer[1])))
            distsock.settimeout(GLOBAL_TIMEOUT)
            message = "DISTRIBUTE_" + str(username_key) + "_LISTENING_" + str(self.port) + "_PUBLICKEY_" + self.public_key + "_" + str(self.ring_position)
            #print("distribute_name() : Sende Nachricht " + message)
            distsock.send(bytes(message, "utf-8"))
            response = distsock.recv(BUFFER_SIZE).split(bytes("_", "utf-8"))
            if str(response[0], "utf-8") == "SUCCESS":
                print("check_distributed_name() : Key war nicht mehr vorhanden, aber wurde neu gesetzt (sollte eigentlich bei Peer " + responsible_peer[0] + ":" + str(responsible_peer[1]) + " gewesen sein).")
                return
            if str(response[0], "utf-8") == "DECRYPT":
                encrypted_message = bytes("_", "utf-8").join(response[1:])
                decrypted_message = decrypt(self.private_key, encrypted_message)
                distsock.send(decrypted_message)
                response = str(distsock.recv(BUFFER_SIZE), "utf-8")
                if response == "SUCCESS":
                    #print("check_distributed_name() : Key ist noch vorhanden und beim richtigen Peer (" + responsible_peer[0] + ":" + str(responsible_peer[1]) + ").")
                    return
                if response == "FAILURE":
                    print("check_distributed_name() : Key kann nicht geändert werden. Möglicherweise ist der verwaltende Peer ausgefallen und jemand anderes hat den Namen angenommen.")
                    print("check_distributed_name() : Anwendung wird beendet")
                    self.shutdown()
                    sys.exit(0)
        except socket.error:
            print("check_distributed_name(): Socket Error.")
            return "ERROR"

    def start_chat(self, remote_ip: str, remote_port: int, remote_name: str):
        print(f"Starting Chord Chat with {remote_ip}:{remote_port}")
        try:
            chatsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            chatsock.settimeout(GLOBAL_TIMEOUT)
            chatsock.connect((remote_ip, remote_port))
            chatsock.send(bytes(f"CHAT_{self.port + 1}_{self.username}", "utf-8"))
            chatsock.close()
            chatsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            chatsock.settimeout(GLOBAL_TIMEOUT)
            chatsock.bind((remote_ip, self.port + 1))
            chatsock.listen(1)
            conn, addr = chatsock.accept()
            self.app.conn_or_socket = conn
            self.app.connected = True
            self.app.chat(remote_name)
        except Exception as msg:
            print(msg)
        return conn

    def connect_chat(self, remote_ip: str, remote_port: int, remote_name: str):
        print(f"Incoming chat from {remote_ip}:{remote_port}")
        try:
            connectsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connectsock.settimeout(GLOBAL_TIMEOUT)
            connectsock.connect((remote_ip, remote_port))
            self.app.conn_or_socket = connectsock
            self.app.connected = True
            self.app.chat(remote_name)
        except Exception as msg:
            print(msg)
        return connectsock

    def give_keys(self, remote_address: tuple, up_until: int):
        keys_to_send = [x for x in list(self.keys) if (up_until - int(x)) % SIZE <= (self.ring_position - int(x)) % SIZE]
        givesock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        givesock.settimeout(GLOBAL_TIMEOUT)
        try:
            givesock.connect(remote_address)
            for x in keys_to_send:
                ip = self.keys[x][0]
                port = str(self.keys[x][1])
                public_key = self.keys[x][2]
                timestamp = self.keys[x][3]
                time_then = datetime.utcfromtimestamp(timestamp)
                if timedelta.total_seconds(datetime.utcnow() - time_then) < KEY_LIFESPAN:
                    print("give_keys(): Übergebe Key " + str(x) + " an " + remote_address[0] + ":" + str(remote_address[1]))
                    givesock.send(bytes("GIVE_" + str(x) + "_" + ip + "_" + port + "_" + public_key + "_" + str(timestamp) + "_" + str(self.ring_position), "utf-8"))
                    time.sleep(0.1)
                else:
                    print("give_keys() : Dropping old username of " + ip + ":" + str(port))
                del self.keys[x]
            givesock.close()
        except socket.error:
            print("give_keys() : Keys konnten nicht an " + remote_address[0] + ":" + str(remote_address[1]) + " übermittelt werden.")
            givesock.close()
            return "ERROR"
        return "SUCCESS"

    def query(self, k, remote_address):
        print(f"query() : Frage Key {k} bei Peer {remote_address} an.")
        querysock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        querysock.settimeout(GLOBAL_TIMEOUT)
        try:
            querysock.connect(remote_address)
            querysock.send(bytes(f"QUERY_{k}", "utf-8"))
            response = str(querysock.recv(BUFFER_SIZE), "utf-8").split("_")
            if response[0] == "ERROR":
                querysock.close()
                return "ERROR"
            elif response[0] == "":
                response[0] = remote_address[0]
            querysock.close()
            return response[0], int(response[1])
        except socket.error:
            print("query(): Socket error")
            querysock.close()
            return "ERROR"

    def server(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.listen(10)
        self.conns = {}
        self.threads = {}
        print("Server : Listening to Port " + str(self.port))
        while not self.shutdown_:
            conn, addr = self.sock.accept()
            self.conns[addr[0] + ":" + str(addr[1])] = conn
            # print(addr[0] + ":" + str(addr[1]) + " connected.")
            cthread = threading.Thread(target=self.client_thread, args=(conn, addr), daemon=True)
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
                # print(addr[0] + ":" + str(addr[1]) + " disconnected (Error).")
                break
            message = str(data, "utf-8")
            if not message:
                conn.close()
                del self.conns[addr[0] + ":" + str(addr[1])]
                del self.threads[addr[0] + ":" + str(addr[1])]
                # print(addr[0] + ":" + str(addr[1]) + " disconnected (No Message).")
                break
            #print("Message from " + addr[0] + ":" + str(addr[1]) + ": " + message)
            msgsplit = message.split("_")
            command = msgsplit[0]
            sending_peer_id = msgsplit[-1]
            response = ""
            if command == "SUCC":
                # print("Server (succ) : Suche nach " + msgsplit[1] + ", auf Anfrage von Peer " + sending_peer_id)
                response = self.succ(int(msgsplit[1]))
                # print("succ() : Key " + msgsplit[1] + " gefunden.")
                # print("Server (succ) : Antworte " + response)
                if self.successor[2] == self.ring_position:
                    print(
                        "notify_successor() : Setze neuen Successor und Finger, (evtl. weil zuvor alleine im Netzwerk): " + addr[
                            0] + ":" + msgsplit[3] + " (" + str(sending_peer_id) + ")")
                    self.successor = [addr[0], int(msgsplit[3]), int(sending_peer_id)]
                    self.fingers[int(sending_peer_id)] = [addr[0], int(msgsplit[3])]
            elif command == "STABILIZE":
                if self.predecessor == []:
                    print("stabilize() : Setze neuen Predecessor da zuvor keiner vorhanden: " + addr[0] + ":" + msgsplit[
                        2] + " (" + str(sending_peer_id) + ")")
                    self.predecessor = [addr[0], int(msgsplit[2]), int(sending_peer_id)]
                response = str(self.predecessor[0]) + "_" + str(self.predecessor[1]) + "_" + str(self.predecessor[2])
            elif command == "PREDECESSOR?":
                if not self.predecessor or self.predecessor[2] == self.ring_position or \
                        (self.ring_position - int(sending_peer_id)) % SIZE < \
                        (self.ring_position - self.predecessor[2]) % SIZE:
                    print("stabilize() : Setze neuen Predecessor: " + addr[0] + ":" + msgsplit[1] + " (" + str(
                        sending_peer_id) + ")")
                    self.predecessor = [addr[0], int(msgsplit[1]), int(sending_peer_id)]
            elif command == "FIXFINGERS":
                pass
            elif command == "PING":
                response = "PONG"
            elif command == "JOINED":
                print("join(): Setze neuen Predecessor: " + addr[0] + ":" + msgsplit[2] + " (" + sending_peer_id + ")")
                # response = self.predecessor[0] + "_" + str(self.predecessor[1]) + "_" + str(self.predecessor[2])
                self.predecessor = [addr[0], int(msgsplit[2]), int(sending_peer_id)]
                self.give_keys((addr[0], int(msgsplit[2])), int(sending_peer_id))
            elif command == "GIVE":
                hash_key = msgsplit[1]
                ip = msgsplit[2]
                if ip == "":
                    ip = addr[0]
                port = int(msgsplit[3])
                public_key = msgsplit[4]
                timestamp = float(msgsplit[5])
                print("give_keys() : Receiving key on position " + hash_key + " from peer " + sending_peer_id)
                self.keys[hash_key] = [ip, port, public_key, timestamp]
            elif command == "DISTRIBUTE":
                remote_hash = msgsplit[1]
                remote_port = msgsplit[3]
                #print("distribute_name() : Public key erhalten: " + msgsplit[5])
                serialized_remote_public_key = msgsplit[5]
                remote_public_key = unserializePublicKey(bytes(serialized_remote_public_key, "utf-8"))
                if not remote_hash in list(self.keys):
                    print("distribute() : Speichere key mit Position " + remote_hash + ", erhalten von " + sending_peer_id)
                    self.keys[remote_hash] = [addr[0], int(remote_port), serialized_remote_public_key, datetime.timestamp(datetime.utcnow())]
                    response = "SUCCESS"
                else:
                    message_to_encrypt = os.urandom(16)
                    #print("distribute_name() : Nachricht, die encrypted wird: ")
                    #print(message_to_encrypt)
                    encrypted_message = encrypt(remote_public_key, message_to_encrypt)
                    self.conns[addr[0] + ":" + str(addr[1])].send(bytes("DECRYPT_", "utf-8") + encrypted_message)
                    decrypted_message = conn.recv(BUFFER_SIZE)
                    #print("distribute_name() : Nachricht von Peer " + sending_peer_id + ": ")
                    #print(decrypted_message)
                    if decrypted_message == message_to_encrypt:
                        if remote_hash in list(self.keys):
                            if not self.keys[remote_hash][0:2] == [addr[0], int(remote_port)]:
                                self.keys[remote_hash][0:2] = [addr[0], int(remote_port)]
                            response = "SUCCESS"
                        else:
                            print("distribute() : Speichere key mit Position " + remote_hash)
                            self.keys[remote_hash] = [addr[0], int(remote_port), serialized_remote_public_key, datetime.timestamp(datetime.utcnow())]
                            response = "SUCCESS"
                    else:
                        response = "FAILURE"
            elif command == "QUERY":
                queried_position = int(msgsplit[1])
                if queried_position in list(self.keys):
                    time_then = datetime.utcfromtimestamp(self.keys[queried_position][3])
                    if timedelta.total_seconds(datetime.utcnow() - time_then) < KEY_LIFESPAN:
                        print("query(): Übergebe angefragten Key " + str(queried_position))
                        response = self.keys[queried_position][0] + "_" + str(self.keys[queried_position][1])
                    else:
                        print("query() : Dropping old key " + str(queried_position))
                        del self.keys[queried_position]
                        response = "ERROR"
                else:
                    print("query(): Angefragter Key nicht vorhanden.")
                    response = "ERROR"
            elif command == "CHAT":
                remote_ip = addr[0]
                remote_port = int(msgsplit[1])
                remote_name = msgsplit[2]
                chat_thread = threading.Thread(target=self.connect_chat, args=(remote_ip, remote_port, remote_name), daemon=True)
                chat_thread.start()
                break
            elif command == "SHUTDOWN":
                if msgsplit[1] == "SUCCESSOR:":
                    print("shutdown() : Setze neuen Successor: " + msgsplit[2] + ":" + msgsplit[3] + " (" + msgsplit[4] + ")")
                    self.successor = [msgsplit[2], int(msgsplit[3]), int(msgsplit[4])]
                elif msgsplit[1] == "PREDECESSOR:":
                    print("shutdown() : Setze neuen Predecessor: " + msgsplit[2] + ":" + msgsplit[3] + " (" + msgsplit[4] + ")")
                    self.predecessor = [msgsplit[2], int(msgsplit[3]), int(msgsplit[4])]

            if response != "":
                #print("Sending response: " + response)
                self.conns[addr[0] + ":" + str(addr[1])].send(bytes(response, "utf-8"))


if __name__ == "__main__":
    local = LocalNode()
    #time.sleep(20)
    #local.shutdown()
    # 192.168.178.20:11111
    # ay-test.duckdns.org:11111
    # Kai : 95.91.208.139:11111
    # while True:
    # print("Active Threads: " + str(threading.active_count()))
    # time.sleep(5)
