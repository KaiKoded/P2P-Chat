import random
import sys
import numpy as np
import hashlib
import time
import threading
import socket
from datetime import datetime, timedelta

from Settings import *
from KeyGen import *

# Decorator für daemons:
def repeat_and_sleep(sleep_time):
    def decorator(func):
        def inner(self, *args, **kwargs):
            while not self.shutdown_:
                time.sleep(sleep_time)
                func(self, *args, **kwargs)
        return inner
    return decorator

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
        # Eigene Adresse ist [IP: str, Port: int, Position: int]
        self.port = port
        self.shutdown_ = False
        self.daemons = {}
        # Predecessor und Successor sind [IP: str, Port: int, Position: int]
        self.predecessor = []
        self.successor = []
        # Fingers sind [Position: int : [IP: str, Port: int]]
        self.fingers = {}
        # DHT Einträge sind {hash: int : [IP: str, Port: int, Public Key: str, Timestamp: float]}
        self.keys = {}
        self.lock = threading.Lock()
        # Aus KeyGen.py:
        self.private_key = getPrivateKey()
        self.public_key = str(serializePublicKey(getPublicKey(self.private_key)), "utf-8")
        self.entry_address = entry_address
        if self.entry_address != "":
            self.entry_address = self.entry_address.split(":")
            self.entry_address = (":".join(self.entry_address[:-1]), int(self.entry_address[-1]))
        self.username = username
        self.ring_position = self.id()
        print(f"Eigene Ringposition = {self.ring_position}")
        self.joined = self.join()
        if self.joined:
            self.start_daemons()

    def hash_username(self, username: str):
        return int(hashlib.sha1(username.encode("utf-8")).hexdigest(), 16) % SIZE

    def id(self):
        return random.randint(0, SIZE - 1)

    def shutdown(self):
        # Flag für die daemons:
        self.shutdown_ = True
        print("shutdown() : Shutdown wird eingeleitet.")
        if self.successor and not self.successor[2] == self.ring_position:
            to_successor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            to_successor.settimeout(GLOBAL_TIMEOUT)
            try:
                to_successor.connect((self.successor[0], self.successor[1]))
                to_successor.send(bytes(
                    "SHUTDOWN_PREDECESSOR:_" + self.predecessor[0] + "_" + str(self.predecessor[1]) + "_" + str(
                        self.predecessor[2]) + "_" + str(self.ring_position), "utf-8"))
            except socket.error:
                print("shutdown() : Fehler beim Senden der Predecessor-Informationen.")
            to_successor.close()
            self.give_keys((self.successor[0], self.successor[1]), self.ring_position)
        if self.predecessor and not self.predecessor[2] == self.ring_position:
            to_predecessor = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            to_predecessor.settimeout(GLOBAL_TIMEOUT)
            try:
                to_predecessor.connect((self.predecessor[0], self.predecessor[1]))
                to_predecessor.send(bytes(
                    "SHUTDOWN_SUCCESSOR:_" + self.successor[0] + "_" + str(self.successor[1]) + "_" + str(
                        self.successor[2]) + "_" + str(self.ring_position), "utf-8"))
            except socket.error:
                print("shutdown() : Fehler beim Senden der Successor-Informationen.")
            to_predecessor.close()
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
            self.daemons[key].daemon = True
            self.daemons[key].start()
        print("Daemons gestartet.")

    def join(self):
        if not self.entry_address:
            # Wenn man keine entry_address eingibt, wird eine neue Chord-DHT gestartet
            # Daher setzt man sich selbst als Successor und Predecessor und speichert seinen Usernamen bei sich selbst
            self.successor = ["", self.port, self.ring_position]
            self.predecessor = ["", self.port, self.ring_position]
            username_key = int(hashlib.sha1(self.username.encode("utf-8")).hexdigest(), 16) % SIZE
            print("join() : Speichere eigenen username mit position " + str(username_key) + " bei sich selbst.")
            self.keys[username_key] = ["", self.port, self.public_key, datetime.timestamp(datetime.utcnow())]
            print("Neue Chord DHT wurde gestartet. Warte auf Verbindungen.")
            return True
        # Folgendes passiert wenn eine entry_address gesetzt wurde:
        # Verteile eigenen Namen in der DHT:
        distribute_status = self.distribute_name(self.entry_address)
        if distribute_status == "ERROR":
            print("join() : Fehler beim Verteilen des Namens.")
            return False
        # Suche nach dem Peer, der zur Zeit noch unsere Ringposition verwaltet:
        succinfo = self.succ(self.ring_position, self.entry_address).split("_")
        if succinfo[0] == "ERROR":
            print("Point of Entry nicht erreichbar. Programm wird beendet.")
            return False
        # Wenn succinfo[0] leer ist, bedeutet das, dass der sendende Peer seine IP nicht wusste.
        # Das wird normalerweise in der succ()-Funktion korrigiert. Wenn aber nicht, bedeutet das, dass genau unser
        # Entry-Peer unsere Ringposition verwaltet und dieser gleichzeitig sein eigener Successor ist.
        # Quasi nur relevant für Peer 1. Wir korrigieren es also hier:
        if not succinfo[0]:
            succinfo[0] = self.entry_address[0]
        self.successor = [succinfo[0], int(succinfo[1]), int(succinfo[2])]
        # Gebe dem gefundenen neuen Successor bescheid, damit er mich als Predecessor setzen kann:
        notify_status = self.notify_successor()
        if notify_status == "ERROR":
            print("join() : Successor kann nicht kontaktiert werden. Bitte erneut versuchen.")
            return False
        print("join() : Successor " + self.successor[0] + ":" + str(self.successor[1]) + " an Position " + str(
            self.successor[2]) + " gefunden.")
        # Berechne alle meine Fingerpositionen:
        finger_positions = (self.ring_position + 2 ** np.arange(0, m)) % SIZE
        # Der successor ist immer ein Finger, also macht es keinen Sinn, dafür extra eine Anfrage raus zu schicken:
        print("join() : Finger " + self.successor[0] + ":" + str(self.successor[1]) + " an Position " + str(
            self.successor[2]) + " gefunden.")
        self.fingers[self.successor[2]] = [self.successor[0], self.successor[1]]
        found = self.successor[2]
        for fpos in finger_positions:
            # Wir brauchen nur eine Anfrage raus schicken, wenn fpos weiter von uns weg liegt als der zuletzt gefundene
            # Finger:
            if (fpos - self.ring_position) % SIZE > (found - self.ring_position) % SIZE:
                info = self.succ(fpos).split("_")
                if info[0] == "ERROR":
                    continue
                found = int(info[2])
                # Der gefundene Peer wird nur als Finger gespeichert, wenn ich ihn noch nicht kenne und ich es nicht
                # selbst bin:
                if not found == self.ring_position and found not in list(self.fingers):
                    print("join() : Finger " + info[0] + ":" + info[1] + " an Position " + info[2] + " gefunden.")
                    self.fingers[found] = [info[0], int(info[1])]
        print("Join abgeschlossen.")
        return True

    def succ(self, k, entry=None, joined=True):
        retries = 0
        while retries < SUCC_RET:
            # Lock, weil mehrere Daemons auf succ() zugreifen. Better threadsafe than threadsorry.
            self.lock.acquire()
            if entry is not None:
                # Wenn eine entry_address an succ übergeben wird, dann wissen wir ja, an welchen Peer wir die Anfrage
                # weiterleiten:
                address_to_connect_to = entry
            else:
                # Wenn nicht, müssen wir erst herausfinden, welcher unserer bekannten Peers am nächsten ist:
                # Zuerst checken wir, ob unser Successor den Key verwaltet. Dann können wir sofort returnen.
                distance_to_successor = (self.successor[2] - self.ring_position) % SIZE
                if distance_to_successor == 0:
                    # Wenn wir unser eigener Successor sind (entweder weil wir alleine im Netzwerk sind oder
                    # kurzzeitig in Ausnahmefällen mit sehr wenigen Peers im Netzwerk), wollen wir uns selbst
                    # zurückgeben. Das führt natürlich potentiell zu Fehlern, falls wir nicht alleine sind und genau
                    # während der kurzen Zeit, in der wir weder Successor noch Finger haben, über uns geroutet wird.
                    # Dies ist aber nur relevant, wenn unser Successor all unsere Finger stellt und ausfällt, was
                    # natürlich relativ unwahrscheinlich ist.
                    distance_to_successor = SIZE
                distance_to_key = (k - self.ring_position) % SIZE
                # Wenn unser successor den key verwaltet, returnen wir ihn:
                if distance_to_key <= distance_to_successor:
                    self.lock.release()
                    return self.successor[0] + "_" + str(self.successor[1]) + "_" + str(self.successor[2])
                # Wenn nicht, gehen wir unsere Finger durch, um zu bestimmen, über wen die Anfrage weiter geleitet wird:
                finger_peers = np.array(list(self.fingers))
                finger_peer_distances_to_key = (k - finger_peers) % SIZE
                try:
                    id_of_closest_finger = int(finger_peers[np.where(finger_peer_distances_to_key == np.min(finger_peer_distances_to_key))])
                    if np.min(finger_peer_distances_to_key) > (k - self.successor[2]) % SIZE:
                        # Wenn wir mit all unseren Fingern den Key überschreiten, routen wir über unseren Successor
                        # weiter:
                        address_to_connect_to = (self.successor[0], self.successor[1])
                    else:
                        # Ansonsten über den nächsten Finger:
                        address_to_connect_to = tuple(self.fingers[id_of_closest_finger])
                except ValueError:
                    # Wenn wir keine Finger haben, routen wir auch über unseren Successor:
                    address_to_connect_to = (self.successor[0], self.successor[1])
            if joined:
                message = "SUCC_" + str(k) + "_LISTENING_" + str(self.port) + "_" + str(self.ring_position)
            else:
                # Diese Flag existiert für den Join eines neuen Peers. Wenn joined == False, dann wird der anfragende
                # Peer nicht als Mitglied des Chord-Rings angesehen. Der Grund dafür ist, dass ein Peer einen neu
                # joinenden Peer sofort als Successor und Finger speichert, wenn er alleine im Netzwerk ist (also vor
                # allem relevant für Peer 0). Wir wollen das aber nicht, weil es sein kann, dass der Name des neu
                # joinenden Peers bereits vergeben ist; dann wird der Eintritt in das Netzwerk nämlich verweigert.
                message = "SUCC_" + str(k) + "_LISTENING_" + str(self.port) + "_NOJOIN_" + str(self.ring_position)
            # Da wir nun den zu kontaktierenden Peer herausgefunden haben, können wir die Nachricht schicken:
            succsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            succsock.settimeout(GLOBAL_TIMEOUT)
            try:
                succsock.connect(address_to_connect_to)
                succsock.send(bytes(message, "utf-8"))
                response = str(succsock.recv(BUFFER_SIZE), "utf-8").split("_")
                # Hier ist die Korrektur, von der oben gesprochen wurde. Wenn response[0] == "" ist, kennt der
                # returnende Peer seine eigene IP nicht. Da succ() kaskadierend ausgeführt wird, MUSS das der Peer sein,
                # den wir selbst kontaktiert haben (sonst hätte ein anderer Peer die Korrektur bereits ausgeführt).
                # Wir können die IP also einfach ersetzen.
                if response[0] == "":
                    response[0] = address_to_connect_to[0]
                response = "_".join(response)
                succsock.close()
                self.lock.release()
                return response
            except socket.error:
                succsock.close()
                self.lock.release()
                print(str(threading.currentThread()) + " : succ() : Socket-Fehler. Versuche erneut...")
                retries += 1
                time.sleep(3)
            if retries == SUCC_RET:
                print("succ() : Anfrage gescheitert!")
                return "ERROR"

    def notify_successor(self):
        # Funktion für den Join. Wir teilen unserem neuen Successor mit, dass wir gejoint sind.
        notisock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        notisock.settimeout(GLOBAL_TIMEOUT)
        try:
            notisock.connect((self.successor[0], self.successor[1]))
            notisock.send(bytes("JOINED_LISTENING_" + str(self.port) + "_" + str(self.ring_position), "utf-8"))
            notisock.close()
            return "SUCCESS"
        except socket.error:
            notisock.close()
            return "ERROR"

    @repeat_and_sleep(CHECK_PREDECESSOR_INT)
    def check_predecessor(self):
        if not self.predecessor:
            # Wenn wir keinen Predecessor haben, gibt es nichts zu checken.
            return
        if self.predecessor[2] == self.ring_position:
            # Das gleiche gilt, wenn wir unser eigener Predecessor sind.
            return
        cpsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        cpsock.settimeout(GLOBAL_TIMEOUT)
        try:
            cpsock.connect((self.predecessor[0], self.predecessor[1]))
        except socket.error:
            print("check_predecessor(): Keine Antwort erhalten. Entferne predecessor " + str(self.predecessor[2]))
            self.predecessor = []
        cpsock.close()

    @repeat_and_sleep(STABILIZE_INT)
    def stabilize(self):
        if self.successor[2] == self.ring_position:
            # Wenn wir unser eigener Successor sind, gibts es nichts zu stabilizen.
            return
        stabsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        stabsock.settimeout(GLOBAL_TIMEOUT)
        try:
            stabsock.connect((self.successor[0], self.successor[1]))
        except socket.error:
            # Falls unser successor nicht antwortet, wird ein neuer gesetzt:
            print("stabilize() : Successor " + str(self.successor[2]) + " antwortet nicht. Setze neuen Successor.")
            stabsock.close()
            self.newsuccessor()
            return
        message = "STABILIZE_LISTENING_" + str(self.port) + "_ID_" + str(self.ring_position)
        stabsock.send(bytes(message, "utf-8"))
        response = str(stabsock.recv(BUFFER_SIZE), "utf-8").split("_")
        if int(response[2]) == self.ring_position:
            # Wenn man sich selbst als Antwort bekommt, ist alles in Ordnung.
            stabsock.close()
            return
        # Wenn man einen anderen Peer als Antwort bekommt, setzt man sich diesen als Successor und sagt ihm bescheid,
        # dass man sein Predecessor sein könnte.
        print("stabilize(): Setze neuen successor: " + response[0] + ":" + response[1] + " (" + response[2] + ")")
        # TODO: Ping bevor Successor setzen
        self.successor = [response[0], int(response[1]), int(response[2])]
        stabsock.close()
        stabsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        stabsock.connect((self.successor[0], self.successor[1]))
        stabsock.send(
            bytes("PREDECESSOR?_" + str(self.port) + "_" + str(self.ring_position), "utf-8"))
        stabsock.close()

    def newsuccessor(self):
        # Wir filtern unseren Successor aus unserer Fingertabelle heraus
        # TODO: Einfach den successor aus der Fingertabelle löschen?
        fingers_without_successor = [x for x in list(self.fingers) if x != self.successor[2]]
        if not fingers_without_successor:
            print(
                "Keine Fingereinträge übrig! Setze sich selbst als Successor, um neu aufgesetztes Chord zu simulieren.")
            self.successor = ["", self.port, self.ring_position]
            return
        finger_positions = np.array(fingers_without_successor)
        # TODO: Ist das nicht falschrum?
        potential_new_successors = (self.ring_position - finger_positions) % SIZE
        new_successor = int(finger_positions[np.where(potential_new_successors == np.min(potential_new_successors))])
        # TODO: Ping bevor Successor setzen
        self.successor = self.fingers[new_successor] + [new_successor]
        print("stabilize() : Neuer Successor: " + str(new_successor))

    @repeat_and_sleep(FIX_FINGERS_INT)
    def fix_fingers(self):
        if self.successor[2] == self.ring_position:
            return
        finger_positions = (self.ring_position + 2 ** np.arange(0, m)) % SIZE
        # finger_diff = False
        found = None
        for fpos in finger_positions:
            # Es wird nur eine Anfrage rausgeschickt, wenn die aktuell zu prüfende Fingerposition weiter weg ist als der zuletzt gefundene Finger:
            if not found or (fpos - self.ring_position) % SIZE > (found - self.ring_position) % SIZE:
                #print(f"fix_fingers() : succ({fpos})")
                info = self.succ(fpos).split("_")
                #print(f"fix_fingers() : Result: {info}")
                if info[0] == "ERROR":
                    continue
                found = int(info[2])
                if found == self.ring_position:
                    break
                if not [info[0], int(info[1])] in self.fingers.values() and not int(info[2]) == self.ring_position:
                    print("fix_fingers(): Finger " + info[0] + ":" + info[1] + " an Position " + info[2] + " gefunden.")
                    self.fingers[int(info[2])] = [info[0], int(info[1])]
        if self.fingers:
            # Überprüfe, ob nun redundante Finger gespeichert sind:
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
        # Pinge nun alle übrigen Finger an:
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

    def distribute_name(self, entry = None):
        username_key = self.hash_username(self.username)
        print("distribute_name() : Username-Ringposition: " + str(username_key))
        #print(f"distribute_name() : succ({username_key}, {entry}, {False})")
        responsible_peer = self.succ(username_key, entry, False).split("_")
        print("distribute_name() : Verantwortlicher Peer: " + responsible_peer[0] + ":" + str(
            responsible_peer[1]) + " (" + str(responsible_peer[2]) + ")")
        if responsible_peer == "ERROR":
            print(
                "distribute_name() : Name konnte nicht verteilt werden, da verantwortlicher Peer nicht erreichbar ist."
                "Join wird abgebrochen.")
            return "ERROR"
        # Port und Ringposition sind Integers:
        # if int(responsible_peer[2]) == self.ring_position or (int(responsible_peer[2]) - username_key) % SIZE > (
        #         self.ring_position - username_key) % SIZE:
        #     self.keys[username_key] = ["", self.port, self.public_key, datetime.timestamp(datetime.utcnow())]
        #     print("distribute_name() : Key wird bei sich selbst gespeichert.")
        #     return
        distsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            distsock.connect((responsible_peer[0], int(responsible_peer[1])))
            distsock.settimeout(GLOBAL_TIMEOUT)
            message = "DISTRIBUTE_" + str(username_key) + "_LISTENING_" + str(
                self.port) + "_PUBLICKEY_" + self.public_key + "_" + str(self.ring_position)
            # print("distribute_name() : Sende Nachricht " + message)
            distsock.send(bytes(message, "utf-8"))
            response = distsock.recv(BUFFER_SIZE).split(bytes("_", "utf-8"))
            if str(response[0], "utf-8") == "SUCCESS":
                distsock.close()
                return "SUCCESS"
            if str(response[0], "utf-8") == "DECRYPT":
                #print("distribute_name() : Identität wird geprüft.")
                encrypted_message = bytes("_", "utf-8").join(response[1:])
                try:
                    decrypted_message = decrypt(self.private_key, encrypted_message)
                except ValueError:
                    distsock.close()
                    return "ERROR"
                distsock.send(decrypted_message)
                response = str(distsock.recv(BUFFER_SIZE), "utf-8")
                if response == "SUCCESS":
                    distsock.close()
                    return "SUCCESS"
                if response == "FAILURE":
                    distsock.close()
                    return "ERROR"
        except socket.error:
            print("distribute_name(): Socket-Fehler.")
            distsock.close()
            return "ERROR"

    @repeat_and_sleep(CHECK_DISTRIBUTE_INT)
    def check_distributed_name(self):
        username_key = self.hash_username(self.username)
        if username_key in list(self.keys):
            # print("check_distributed_name() : Key ist bei sich selbst gespeichert.")
            return
        #print(f"check_distributed_name() : succ({username_key})")
        responsible_peer = self.succ(username_key).split("_")
        if responsible_peer[0] == "ERROR":
            print(
                "check_distributed_name() : Verantwortlicher Peer ist offenbar ausgefallen. Name wird bei der nächsten Iteration neu gesetzt.")
            return
        # Sollte niemals eintreten:
        if (int(responsible_peer[2]) - username_key) % SIZE > (self.ring_position - username_key) % SIZE:
            print("check_distributed_name() : Key ist beim falschen Peer gespeichert!")
            return
        distsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        try:
            distsock.connect((responsible_peer[0], int(responsible_peer[1])))
            distsock.settimeout(GLOBAL_TIMEOUT)
            message = "DISTRIBUTE_" + str(username_key) + "_LISTENING_" + str(
                self.port) + "_PUBLICKEY_" + self.public_key + "_" + str(self.ring_position)
            # print("distribute_name() : Sende Nachricht " + message)
            distsock.send(bytes(message, "utf-8"))
            response = distsock.recv(BUFFER_SIZE).split(bytes("_", "utf-8"))
            if str(response[0], "utf-8") == "SUCCESS":
                print(
                    "check_distributed_name() : Key " + str(username_key) + " war nicht mehr vorhanden, aber wurde neu gesetzt (sollte eigentlich bei Peer " +
                    responsible_peer[0] + ":" + responsible_peer[1] + " (" + responsible_peer[2] + ") gewesen sein).")
                distsock.close()
                return
            if str(response[0], "utf-8") == "DECRYPT":
                #print("distribute_name() : Identität wird geprüft.")
                encrypted_message = bytes("_", "utf-8").join(response[1:])
                try:
                    decrypted_message = decrypt(self.private_key, encrypted_message)
                except ValueError:
                    print(
                        "check_distributed_name() : Key kann nicht geändert werden. Möglicherweise ist der verwaltende Peer ausgefallen und jemand anderes hat den Namen angenommen.")
                    print("check_distributed_name() : Anwendung wird beendet")
                    distsock.close()
                    self.shutdown()
                    return
                distsock.send(decrypted_message)
                response = str(distsock.recv(BUFFER_SIZE), "utf-8")
                if response == "SUCCESS":
                    # print("check_distributed_name() : Key ist noch vorhanden und beim richtigen Peer (" + responsible_peer[0] + ":" + str(responsible_peer[1]) + ").")
                    distsock.close()
                    return
                if response == "FAILURE":
                    print(
                        "check_distributed_name() : Key kann nicht geändert werden. Möglicherweise ist der verwaltende Peer ausgefallen und jemand anderes hat den Namen angenommen.")
                    print("check_distributed_name() : Anwendung wird beendet")
                    distsock.close()
                    self.shutdown()
                    return
        except socket.error:
            print("check_distributed_name(): Socket-Fehler")
            distsock.close()
            return

    def start_chat(self, remote_ip: str, remote_port: int):
        print(f"Chord-Chat mit {remote_ip}:{remote_port} wird gestartet.")
        try:
            chatsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            chatsock.settimeout(GLOBAL_TIMEOUT)
            chatsock.connect((remote_ip, remote_port))
            chatsock.send(bytes(f"CHAT_{self.port + 1}_{self.username}", "utf-8"))
            chatsock.close()
            chatsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            chatsock.settimeout(GLOBAL_TIMEOUT)
            chatsock.bind(("0.0.0.0", self.port + 1))
            chatsock.listen(1)
            conn, addr = chatsock.accept()
            self.app.conn_or_socket = conn
            self.app.socket = chatsock
            self.app.connected = True
            self.app.chat(self.app.friend_name)
        except Exception as msg:
            print(msg)
        return

    def connect_chat(self, remote_ip: str, remote_port: int, remote_name: str):
        print(f"Eingehender Chat mit {remote_ip}:{remote_port}")
        try:
            connectsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            connectsock.settimeout(GLOBAL_TIMEOUT)
            connectsock.connect((remote_ip, remote_port))
            self.app.conn_or_socket = connectsock
            self.app.connected = True
            self.app.friend_name = remote_name
            self.app.chat(remote_name)
        except Exception as msg:
            print(msg)
        return

    def give_keys(self, remote_address: tuple, up_until: int):
        keys_to_send = [x for x in list(self.keys) if
                        (up_until - int(x)) % SIZE <= (self.ring_position - int(x)) % SIZE]
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
                    print("give_keys(): Übergebe Key " + str(x) + " an " + remote_address[0] + ":" + str(
                        remote_address[1]))
                    givesock.send(bytes(
                        "GIVE_" + str(x) + "_" + ip + "_" + port + "_" + public_key + "_" + str(timestamp) + "_" + str(
                            self.ring_position), "utf-8"))
                    time.sleep(0.1)
                else:
                    print("give_keys() : Verwerfe alten Usernamen von " + ip + ":" + str(port) + " an Position " + str(x))
                del self.keys[x]
            givesock.close()
        except socket.error:
            print("give_keys() : Keys konnten nicht an " + remote_address[0] + ":" + str(
                remote_address[1]) + " übermittelt werden.")
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
            print("query(): Socket-Fehler")
            querysock.close()
            return "ERROR"

    def server(self):
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.listen(10)
        self.conns = {}
        print("Server : Höre zu auf Port " + str(self.port))
        while not self.shutdown_:
            conn, addr = self.sock.accept()
            self.conns[addr[0] + ":" + str(addr[1])] = conn
            # print(addr[0] + ":" + str(addr[1]) + " connected.")
            cthread = threading.Thread(target=self.client_thread, args=(conn, addr), daemon=True)
            cthread.start()
        self.sock.close()

    def client_thread(self, conn, addr):
        while True:
            try:
                data = conn.recv(BUFFER_SIZE)
            except socket.error:
                conn.close()
                del self.conns[addr[0] + ":" + str(addr[1])]
                # print(addr[0] + ":" + str(addr[1]) + " disconnected (Error).")
                break
            message = str(data, "utf-8")
            if not message:
                conn.close()
                del self.conns[addr[0] + ":" + str(addr[1])]
                # print(addr[0] + ":" + str(addr[1]) + " disconnected (No Message).")
                break
            #print("Message from " + addr[0] + ":" + str(addr[1]) + ": " + message)
            msgsplit = message.split("_")
            command = msgsplit[0]
            sending_peer_id = msgsplit[-1]
            response = ""
            if command == "SUCC":
                # print("Server (succ) : Suche nach " + msgsplit[1] + ", auf Anfrage von Peer " + sending_peer_id)
                #print(f"client_thread() : succ({msgsplit[1]})")
                response = self.succ(int(msgsplit[1]))
                # print("succ() : Key " + msgsplit[1] + " gefunden.")
                # print("Server (succ) : Antworte " + response)
                if self.successor[2] == self.ring_position and not msgsplit[4] == "NOJOIN":
                    print(
                        "notify_successor() : Setze neuen Successor und Finger, (evtl. weil zuvor alleine im Netzwerk): " +
                        addr[
                            0] + ":" + msgsplit[3] + " (" + str(sending_peer_id) + ")")
                    self.successor = [addr[0], int(msgsplit[3]), int(sending_peer_id)]
                    self.fingers[int(sending_peer_id)] = [addr[0], int(msgsplit[3])]
            elif command == "STABILIZE":
                if self.predecessor == []:
                    print(
                        "stabilize() : Setze neuen Predecessor, da zuvor keiner vorhanden: " + addr[0] + ":" + msgsplit[
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
                hash_key = int(msgsplit[1])
                ip = msgsplit[2]
                if ip == "":
                    ip = addr[0]
                port = int(msgsplit[3])
                public_key = msgsplit[4]
                timestamp = float(msgsplit[5])
                print("give_keys() : Erhalte Key an Position " + str(hash_key) + " von Peer " + ip + ":" + str(port) + " (" + sending_peer_id + ")")
                self.keys[hash_key] = [ip, port, public_key, timestamp]
            elif command == "DISTRIBUTE":
                remote_hash = int(msgsplit[1])
                remote_port = msgsplit[3]
                # print("distribute_name() : Public key erhalten: " + msgsplit[5])
                serialized_remote_public_key = msgsplit[5]
                #remote_public_key = unserializePublicKey(bytes(serialized_remote_public_key, "utf-8"))
                if remote_hash not in list(self.keys):
                    print("distribute_name() : Speichere key mit Position " + str(
                        remote_hash) + ", erhalten von " + addr[0] + ":" + remote_port + " (" + sending_peer_id + ")")
                    self.keys[remote_hash] = [addr[0], int(remote_port), serialized_remote_public_key,
                                              datetime.timestamp(datetime.utcnow())]
                    response = "SUCCESS"
                else:
                    #print("distribute_name() : Identität von Peer " + addr[0] + ":" + remote_port + " (" + sending_peer_id + ")" + " wird überprüft.")
                    message_to_encrypt = os.urandom(16)
                    pubkey_from_value = unserializePublicKey(bytes(self.keys[remote_hash][2], "utf-8"))
                    # print("distribute_name() : Nachricht, die encrypted wird: ")
                    # print(message_to_encrypt)
                    encrypted_message = encrypt(pubkey_from_value, message_to_encrypt)
                    self.conns[addr[0] + ":" + str(addr[1])].send(bytes("DECRYPT_", "utf-8") + encrypted_message)
                    decrypted_message = conn.recv(BUFFER_SIZE)
                    # print("distribute_name() : Nachricht von Peer " + sending_peer_id + ": ")
                    # print(decrypted_message)
                    if decrypted_message == message_to_encrypt:
                        #print("distribute_name() : Überprüfung erfolgreich. Erneuere key mit Position " + str(remote_hash))
                        self.keys[remote_hash] = [addr[0], int(remote_port), serialized_remote_public_key,
                                                  datetime.timestamp(datetime.utcnow())]
                        response = "SUCCESS"
                    else:
                        print("distribute_name() : Peer " + addr[0] + ":" + remote_port + " (" + sending_peer_id + ") konnte sich nicht korrekt identifizieren!")
                        response = "FAILURE"
            elif command == "QUERY":
                queried_position = int(msgsplit[1])
                if queried_position in list(self.keys):
                    time_then = datetime.utcfromtimestamp(self.keys[queried_position][3])
                    if timedelta.total_seconds(datetime.utcnow() - time_then) < KEY_LIFESPAN:
                        print("query(): Übergebe angefragten Key " + str(queried_position))
                        response = self.keys[queried_position][0] + "_" + str(self.keys[queried_position][1])
                    else:
                        print("query() : Alter Key " + str(queried_position) + " wird verworfen.")
                        del self.keys[queried_position]
                        response = "ERROR"
                else:
                    print("query(): Angefragter Key nicht vorhanden.")
                    response = "ERROR"
            elif command == "CHAT":
                remote_ip = addr[0]
                remote_port = int(msgsplit[1])
                remote_name = msgsplit[2]
                chat_thread = threading.Thread(target=self.connect_chat, args=(remote_ip, remote_port, remote_name),
                                               daemon=True)
                chat_thread.start()
                break
            elif command == "SHUTDOWN":
                if msgsplit[1] == "SUCCESSOR:":
                    print("shutdown() : Setze neuen Successor: " + msgsplit[2] + ":" + msgsplit[3] + " (" + msgsplit[
                        4] + ")")
                    self.successor = [msgsplit[2], int(msgsplit[3]), int(msgsplit[4])]
                elif msgsplit[1] == "PREDECESSOR:":
                    print("shutdown() : Setze neuen Predecessor: " + msgsplit[2] + ":" + msgsplit[3] + " (" + msgsplit[
                        4] + ")")
                    self.predecessor = [msgsplit[2], int(msgsplit[3]), int(msgsplit[4])]
                if int(sending_peer_id) in list(self.fingers):
                    print("shutdown() : Finger " + sending_peer_id + " wird gelöscht.")
                    del self.fingers[int(sending_peer_id)]
            if response != "":
                # print("Sending response: " + response)
                self.conns[addr[0] + ":" + str(addr[1])].send(bytes(response, "utf-8"))
