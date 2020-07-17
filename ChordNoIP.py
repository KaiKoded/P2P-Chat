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
            # Alle Keys werden an den Successor gereicht:
            self.give_keys((self.successor[0], self.successor[1]), self.ring_position)
            # Anschliessend wird dem Successor der eigene Predecessor mitgeteilt:
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
        if self.predecessor and not self.predecessor[2] == self.ring_position:
            # Nun wird dem eigenen Predecessor noch der eigene Successor mitgeteilt:
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
                # Wenn wir bei der Fingersuche uns selbst finden, sind wir einmal im Kreis gelaufen und können
                # abbrechen.
                if found == self.ring_position:
                    break
                # Der gefundene Peer wird nur als Finger gespeichert, wenn ich ihn noch nicht kenne:
                if found not in list(self.fingers):
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
                # weiterleiten (relevant nur für das Bootstrapping):
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
                finger_peers = np.array(list(self.fingers), dtype=int)
                finger_peer_distances_to_key = (k - finger_peers) % SIZE
                try:
                    id_of_closest_finger = int(
                        finger_peers[np.where(finger_peer_distances_to_key == np.min(finger_peer_distances_to_key))])
                    if np.min(finger_peer_distances_to_key) > (k - self.successor[2]) % SIZE:
                        # Wenn wir mit all unseren Fingern den Key überschreiten, routen wir über unseren Successor
                        # weiter (falls aus irgendeinem Grund unser Successor nicht in unserer Fingertabelle ist,
                        # was eigentlich nur passiert, wenn unser Successor ausfällt, wir einen neuen erhalten,
                        # aber fix_fingers() noch nicht hinterher kam):
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
                time.sleep(2)
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
        stabsock.close()
        if int(response[2]) == self.ring_position:
            # Wenn man sich selbst als Antwort bekommt, ist alles in Ordnung.
            return
        # Wenn man einen anderen Peer als Antwort bekommt, setzt man sich diesen als Successor und sagt ihm bescheid,
        # dass man sein Predecessor sein könnte. Falls er jedoch nicht erreichbar ist, wird nichts unternommen.
        pinger = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        pinger.settimeout(GLOBAL_TIMEOUT)
        try:
            pinger.connect((response[0], int(response[1])))
            pinger.send(bytes("PREDECESSOR?_" + str(self.port) + "_" + str(self.ring_position), "utf-8"))
            pinger.close()
        except socket.error:
            pinger.close()
            return
        print("stabilize(): Setze neuen successor: " + response[0] + ":" + response[1] + " (" + response[2] + ")")
        self.successor = [response[0], int(response[1]), int(response[2])]

    def newsuccessor(self):
        # Diese Funktion wird nur aufgerufen, wenn unser Successor nicht antwortet. Wir gehen also davon aus, dass er
        # ausgefallen ist und löschen ihn direkt auch aus unserer Fingertabelle, sofern er darin ist. (Er könnte fehlen,
        # weil fix_fingers() seit seinem Beitritt noch nicht ausgeführt wurde und er in der kurzen Zeit direkt
        # ausgefallen ist.)
        if self.successor[2] in list(self.fingers):
            print("newsuccessor() : Finger " + str(self.successor[2]) + " wird gelöscht.")
            del self.fingers[self.successor[2]]
        if not self.fingers:
            print("newsuccessor() : Keine Fingereinträge übrig! Setze sich selbst als Successor, um neu aufgesetztes "
                  "Chord zu simulieren.")
            self.successor = ["", self.port, self.ring_position]
            return
        # Der am nächsten liegende Finger wird determiniert...
        finger_positions = np.array(list(self.fingers), dtype=int)
        potential_new_successors = (finger_positions - self.ring_position) % SIZE
        new_successor = int(finger_positions[np.where(potential_new_successors == np.min(potential_new_successors))])
        # ... und als Successor gesetzt
        self.successor = self.fingers[new_successor] + [new_successor]
        print("newsuccessor() : Neuer Successor: " + str(new_successor))

    @repeat_and_sleep(FIX_FINGERS_INT)
    def fix_fingers(self):
        finger_positions = (self.ring_position + 2 ** np.arange(0, m)) % SIZE
        found = None
        for fpos in finger_positions:
            # Wir brauchen nur eine Anfrage raus schicken, wenn fpos weiter von uns weg liegt als der zuletzt gefundene
            # Finger:
            if not found or (fpos - self.ring_position) % SIZE > (found - self.ring_position) % SIZE:
                info = self.succ(fpos).split("_")
                if info[0] == "ERROR":
                    continue
                found = int(info[2])
                # Wenn wir bei der Fingersuche uns selbst finden, sind wir einmal im Kreis gelaufen und können
                # abbrechen.
                if found == self.ring_position:
                    break
                if not [info[0], int(info[1])] in self.fingers.values():
                    print("fix_fingers(): Finger " + info[0] + ":" + info[1] + " an Position " + info[2] + " gefunden.")
                    self.fingers[int(info[2])] = [info[0], int(info[1])]
        if self.fingers:
            # Überprüfe nun, ob redundante Finger gespeichert sind:
            # Wir checken, wie oft jeder gespeicherte Finger am nächsten an jeder theoretischen Fingerposition ist
            times_minimum = np.zeros_like(list(self.fingers))
            for fpos in finger_positions:
                distances = (np.array(list(self.fingers), dtype=int) - fpos) % SIZE
                times_minimum[np.where(distances == np.min(distances))] += 1
            # Wenn ein Finger nie am nächsten dran war, wird er gelöscht
            to_delete = np.where(times_minimum == 0)[0]
            # Wir ersetzen die Liste mit den eigentlichen Keys, um nicht in Indexing-Probleme zu geraten:
            to_delete = np.array(list(self.fingers))[to_delete].tolist()
            if to_delete:
                for d in to_delete:
                    print("fix_fingers(): Lösche redundanten Finger " + self.fingers[d][0] + ":" +
                          str(self.fingers[d][1]) + " (" + str(d) + ")")
                    del self.fingers[d]
        # Pinge nun alle übrigen Finger an:
        for finger in list(self.fingers):
            ffsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            ffsock.settimeout(GLOBAL_TIMEOUT)
            try:
                ffsock.connect((self.fingers[finger][0], self.fingers[finger][1]))
            except socket.error:
                print(
                    "fix_fingers(): Keine Antwort erhalten. Entferne finger " + self.fingers[finger][0] + ":" + str(
                        self.fingers[finger][1]) + " (" + str(finger) + ")")
                del self.fingers[finger]
            ffsock.close()

    def distribute_name(self, entry=None):
        username_key = self.hash_username(self.username)
        print("distribute_name() : Username-Ringposition: " + str(username_key))
        # Die False-Flag in succ() sagt aus, dass der anfragende Peer noch nicht dem Netzwerk gejoint ist, sondern nur
        # seinen Namen anfragt:
        responsible_peer = self.succ(username_key, entry, False).split("_")
        print("distribute_name() : Verantwortlicher Peer: " + responsible_peer[0] + ":" + str(
            responsible_peer[1]) + " (" + str(responsible_peer[2]) + ")")
        if responsible_peer == "ERROR":
            print(
                "distribute_name() : Name konnte nicht verteilt werden, da verantwortlicher Peer nicht erreichbar ist."
                "Join wird abgebrochen.")
            return "ERROR"
        # Nachdem wir den verwaltenden Peer herausgefunden haben, teilen wir ihm unseren (gehashten) Namen mit:
        distsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        distsock.settimeout(GLOBAL_TIMEOUT)
        try:
            distsock.connect((responsible_peer[0], int(responsible_peer[1])))
            message = "DISTRIBUTE_" + str(username_key) + "_LISTENING_" + str(
                self.port) + "_PUBLICKEY_" + self.public_key + "_" + str(self.ring_position)
            distsock.send(bytes(message, "utf-8"))
            # Beim splitten der response muss aufgepasst werden. Wir splitten den Bytestring, da der verschlüsselte
            # String, der zur Identitätsüberprüfung empfangen wird, nicht UTF-8-kodiert werden kann.
            response = distsock.recv(BUFFER_SIZE).split(bytes("_", "utf-8"))
            # Wenn der Name noch nicht vorhanden war, erhalten wir direkt ein "SUCCESS"
            if str(response[0], "utf-8") == "SUCCESS":
                distsock.close()
                return "SUCCESS"
            # Wenn der Name schon vorhanden war, müssen wir unsere Identität beweisen, indem wir einen verschlüsselten
            # String entschlüsseln. Zur Verschlüsselung wurde der Public Key im Schlüssel-Wert-Paar benutzt und er muss
            # auf unseren Private Key passen.
            if str(response[0], "utf-8") == "DECRYPT":
                # Wir entfernen das "DECRYPT"-Kommando, aber da im verschlüsselten Bytestring auch Unterstiche vorkommen
                # können, müssen wir beachten, dass die nach Unterstrichen gesplittete response-Liste mehr als zwei
                # Objekte enthalten kann.
                encrypted_message = bytes("_", "utf-8").join(response[1:])
                try:
                    decrypted_message = decrypt(self.private_key, encrypted_message)
                # Wenn die Entschlüsselung fehlschlägt, erhalten wir einen ValueError
                except ValueError:
                    distsock.close()
                    return "ERROR"
                # Ist sie erfolgreich, wird das Ergebnis wieder zum verwaltenden Peer geschickt.
                distsock.send(decrypted_message)
                response = str(distsock.recv(BUFFER_SIZE), "utf-8")
                distsock.close()
                if response == "SUCCESS":
                    return "SUCCESS"
                else:
                    return "ERROR"
        except socket.error:
            print("distribute_name(): Socket-Fehler.")
            distsock.close()
            return "ERROR"

    @repeat_and_sleep(CHECK_DISTRIBUTE_INT)
    def check_distributed_name(self):
    # Äquivalent zu distribute_name(), wird aber nicht zum Join ausgeführt, sondern als Daemon, der alle 20s checkt,
    # ob der eigene Name noch ordentlich im Netzwerk verteilt ist.
        username_key = self.hash_username(self.username)
        if username_key in list(self.keys):
            # Wenn ich meinen eigenen Key halte, gibt es nichts zu checken.
            return
        responsible_peer = self.succ(username_key).split("_")
        if responsible_peer[0] == "ERROR":
            print(
                "check_distributed_name() : Verantwortlicher Peer ist offenbar ausgefallen. Name wird bei der nächsten "
                "Iteration neu gesetzt.")
            return
        # Im Grunde passiert jetzt eine leicht abgewandelte Version von distribute_name()
        distsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        distsock.settimeout(GLOBAL_TIMEOUT)
        try:
            distsock.connect((responsible_peer[0], int(responsible_peer[1])))
            message = "DISTRIBUTE_" + str(username_key) + "_LISTENING_" + str(
                self.port) + "_PUBLICKEY_" + self.public_key + "_" + str(self.ring_position)
            distsock.send(bytes(message, "utf-8"))
            response = distsock.recv(BUFFER_SIZE).split(bytes("_", "utf-8"))
            # Wenn vom verantwortlichen Peer ein sofortiges "SUCCESS" zurück kommt, bedeutet das, dass er den Key noch
            # nicht gespeichert hatte, was er aber eigentlich haben sollte. Offenbar ist sein Predecessor ausgefallen,
            # der den Key zuvor hielt.
            if str(response[0], "utf-8") == "SUCCESS":
                print(
                    "check_distributed_name() : Key " + str(
                        username_key) + " war nicht mehr vorhanden, aber wurde neu gesetzt (sollte eigentlich bei Peer " +
                    responsible_peer[0] + ":" + responsible_peer[1] + " (" + responsible_peer[2] + ") gewesen sein).")
                distsock.close()
                return
            # Wenn die Identität geprüft wird, verläuft alles nach Plan
            if str(response[0], "utf-8") == "DECRYPT":
                encrypted_message = bytes("_", "utf-8").join(response[1:])
                try:
                    decrypted_message = decrypt(self.private_key, encrypted_message)
                # Kriegen wir jedoch einen ValueError, bedeutet das, dass jemand anderes unseren Namen gestohlen hat.
                # Dies kann nur passieren, wenn der verwaltende Peer ausfällt und genau in der sleep time dieses Daemons
                # ein neuer Peer den Namen annimmt. In diesem - sofern Zufall - äußerst unwahrscheinlichen Fall wird der
                # Peer aus dem Netzwerk gekickt.
                except ValueError:
                    print(
                        "check_distributed_name() : Key kann nicht geändert werden. Möglicherweise ist der verwaltende "
                        "Peer ausgefallen und jemand anderes hat den Namen angenommen. Anwendung wird beendet.")
                    distsock.close()
                    self.shutdown()
                    return
                distsock.send(decrypted_message)
                response = str(distsock.recv(BUFFER_SIZE), "utf-8")
                if response == "SUCCESS":
                    # Alles ist in Ordnung.
                    distsock.close()
                    return
                else:
                    # Irgendwas anderes ist schief gelaufen. Sollte eigentlich niemals eintreten.
                    print("check_distributed_name() : Irgendetwas ist schief gelaufen. Anwendung wird beendet.")
                    distsock.close()
                    self.shutdown()
                    return
        except socket.error:
            print("check_distributed_name(): Socket-Fehler")
            distsock.close()
            return

    def give_keys(self, remote_address: tuple, up_until: int):
        # Wir senden alle Keys bis zu einer bestimmten Ringposition (könnte die eigene sein im Falle eines Shutdowns,
        # oder nur ein Teil im Falle eines Joins).
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
                # Bei jeder Key-Übergabe checken wir, ob der Timestamp schon abgelaufen ist. Falls ja, wird der Key
                # stattdessen gedroppt.
                time_then = datetime.utcfromtimestamp(timestamp)
                if timedelta.total_seconds(datetime.utcnow() - time_then) < KEY_LIFESPAN:
                    print("give_keys(): Übergebe Key " + str(x) + " an " + remote_address[0] + ":" + str(
                        remote_address[1]))
                    givesock.send(bytes(
                        "GIVE_" + str(x) + "_" + ip + "_" + port + "_" + public_key + "_" + str(timestamp) + "_" + str(
                            self.ring_position), "utf-8"))
                    time.sleep(0.1)
                else:
                    print(
                        "give_keys() : Verwerfe alten Usernamen von " + ip + ":" + str(port) + " an Position " + str(x))
                # In jedem Fall muss der Key bei sich selbst gelöscht werden.
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
                print(f"query() : Query nach Key {k} fehlgeschlagen.")
                querysock.close()
                return "ERROR"
            elif response[0] == "":
                # Hier ist wieder die Korrektur falls der verantwortliche Peer seine eigenen Informationen verwaltet.
                # Da er seine eigene IP nicht kennt, wird sie hier ersetzt:
                response[0] = remote_address[0]
            querysock.close()
            print(f"query() : Antwort: {response}")
            return response[0], int(response[1])
        except socket.error:
            print("query(): Socket-Fehler")
            querysock.close()
            return "ERROR"

    def start_chat(self, remote_ip: str, remote_port: int):
        print(f"Chord-Chat mit {remote_ip}:{remote_port} wird gestartet.")
        chatsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        chatsock.settimeout(GLOBAL_TIMEOUT)
        try:
            # Wollen wir eine Chat-Verbindung aufbauen, sagen wir demjenigen Peer erst bescheid und warten dann darauf,
            # dass er sich mit uns verbindet. Wir teilen ihm außerdem unseren Username mit.
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
            chatsock.close()

    def connect_chat(self, remote_ip: str, remote_port: int, remote_name: str):
        # Diese Funktion führt der Peer aus, der die Chatanfrage erhalten hat.
        print(f"Eingehender Chat mit {remote_ip}:{remote_port}")
        connectsock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        connectsock.settimeout(GLOBAL_TIMEOUT)
        try:
            connectsock.connect((remote_ip, remote_port))
            self.app.conn_or_socket = connectsock
            self.app.connected = True
            self.app.friend_name = remote_name
            self.app.chat(remote_name)
        except Exception as msg:
            print(msg)
            connectsock.close()

    def server(self):
        # Multithreaded server, der Client Threads spawnt, die Anfragen verwalten.
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.port))
        self.sock.listen(10)
        self.conns = {}
        print("Server : Höre zu auf Port " + str(self.port))
        while not self.shutdown_:
            conn, addr = self.sock.accept()
            self.conns[addr[0] + ":" + str(addr[1])] = conn
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
                break
            message = str(data, "utf-8")
            if not message:
                conn.close()
                del self.conns[addr[0] + ":" + str(addr[1])]
                break
            # print("Message from " + addr[0] + ":" + str(addr[1]) + ": " + message)
            msgsplit = message.split("_")
            command = msgsplit[0]
            sending_peer_id = msgsplit[-1]
            response = ""
            if command == "SUCC":
                # Hier wird die Kaskadierung deutlich; der Server führt selbst ein succ() aus, bis die gesuchte
                # Ringposition von jemandes Successor verwaltet wird.
                response = self.succ(int(msgsplit[1]))
                # Falls ich mein eigener Successor bin, kann ich die Gelegenheit nutzen und den anfragenden Peer zu
                # meinem Successor und Finger machen, sofern er dem Netzwerk gejoint ist.
                # Fragt er nur von außen einen Key an, wird das durch die "NOJOIN"-Flag angezeigt.
                if self.successor[2] == self.ring_position and not msgsplit[4] == "NOJOIN":
                    print("notify_successor() : Setze neuen Successor und Finger, (evtl. weil zuvor alleine im "
                          "Netzwerk): " + addr[0] + ":" + msgsplit[3] + " (" + str(sending_peer_id) + ")")
                    self.successor = [addr[0], int(msgsplit[3]), int(sending_peer_id)]
                    self.fingers[int(sending_peer_id)] = [addr[0], int(msgsplit[3])]
            elif command == "STABILIZE":
                # Falls ein stabilize() rein kommt und ich keinen Predecessor habe, setze ich mir den anfragenden Peer
                # als solchen.
                if not self.predecessor:
                    print("stabilize() : Setze neuen Predecessor, da zuvor keiner vorhanden: " + addr[0] + ":" +
                          msgsplit[2] + " (" + str(sending_peer_id) + ")")
                    self.predecessor = [addr[0], int(msgsplit[2]), int(sending_peer_id)]
                # Die Antwort ist der eigene Predecessor:
                response = str(self.predecessor[0]) + "_" + str(self.predecessor[1]) + "_" + str(self.predecessor[2])
            elif command == "PREDECESSOR?":
                # Falls ein Peer mich durch einen stabilize() entdeckt hat, wird er mir eine "PREDECESSOR?"-Anfrage
                # schicken. Ich nehme den neuen Peer als Predecessor an, wenn eine der drei Bedingungen erfüllt ist:
                # 1.) Ich habe keinen Predecessor
                # 2.) Ich bin mein eigener Predecessor (ich war also alleine im Netzwerk, bzw. dachte dies)
                # 3.) Er ist näher an mir dran als mein bisheriger Predecessor
                if not self.predecessor or self.predecessor[2] == self.ring_position or \
                        (self.ring_position - int(sending_peer_id)) % SIZE < \
                        (self.ring_position - self.predecessor[2]) % SIZE:
                    print("stabilize() : Setze neuen Predecessor: " + addr[0] + ":" + msgsplit[1] + " (" + str(
                        sending_peer_id) + ")")
                    self.predecessor = [addr[0], int(msgsplit[1]), int(sending_peer_id)]
            elif command == "JOINED":
                # Ist ein neuer Peer hinter mich gejoint, ist er sicher mein neuer Predecessor, sonst hätte er mich
                # nicht gefunden.
                print("join(): Setze neuen Predecessor: " + addr[0] + ":" + msgsplit[2] + " (" + sending_peer_id + ")")
                self.predecessor = [addr[0], int(msgsplit[2]), int(sending_peer_id)]
                # Ich übergebe ihm meine Keys bis zu seiner Ringposition:
                self.give_keys((addr[0], int(msgsplit[2])), int(sending_peer_id))
            elif command == "GIVE":
                hash_key = int(msgsplit[1])
                ip = msgsplit[2]
                if ip == "":
                    # Wieder die IP-Korrektur
                    ip = addr[0]
                value_port = int(msgsplit[3])
                giver_port = None
                public_key = msgsplit[4]
                timestamp = float(msgsplit[5])
                # give_keys() passiert nur beim Join oder beim Shutdown, also ist der sendende Peer entweder unser
                # Successor oder Predecessor (oder beides).
                if self.successor[2] == int(sending_peer_id):
                    giver_port = self.successor[1]
                elif self.predecessor[2] == int(sending_peer_id):
                    giver_port = self.predecessor[1]
                print("give_keys() : Erhalte Key an Position " + str(hash_key) + " von Peer " + addr[0] + ":" + str(
                    giver_port) + " (" + sending_peer_id + ")")
                self.keys[hash_key] = [ip, value_port, public_key, timestamp]
            elif command == "DISTRIBUTE":
                remote_hash_key = int(msgsplit[1])
                remote_port = msgsplit[3]
                serialized_remote_public_key = msgsplit[5]
                if remote_hash_key not in list(self.keys):
                    # Wenn der Name noch nicht vorhanden ist, wird er gespeichert:
                    print("distribute_name() : Speichere key mit Position " + str(remote_hash_key) + ", erhalten von "
                          + addr[0] + ":" + remote_port + " (" + sending_peer_id + ")")
                    self.keys[remote_hash_key] = [addr[0], int(remote_port), serialized_remote_public_key,
                                                  datetime.timestamp(datetime.utcnow())]
                    response = "SUCCESS"
                else:
                    # Wenn der Name bereits vorhanden ist, wird die Identität des verteilenden Peers geprüft:
                    # Generiere einen zufälligen Bytestring:
                    message_to_encrypt = os.urandom(16)
                    # Der public key, der im Key-Value Paar des Namens nur ein String war, wird jetzt zum cryptography-
                    # Objekt, damit man damit Dinge verschlüsseln kann:
                    pubkey_from_value = unserializePublicKey(bytes(self.keys[remote_hash_key][2], "utf-8"))
                    # Der Bytestring wird verschlüsselt und verschickt:
                    encrypted_message = encrypt(pubkey_from_value, message_to_encrypt)
                    self.conns[addr[0] + ":" + str(addr[1])].send(bytes("DECRYPT_", "utf-8") + encrypted_message)
                    conn.settimeout(GLOBAL_TIMEOUT)
                    # Hier kommt nur etwas an, wenn der Entschlüsselungsvorgang erfolgreich ist:
                    try:
                        decrypted_message = conn.recv(BUFFER_SIZE)
                    except socket.error:
                        print("distribute_name() : Authorisierung fehlgeschlagen!")
                        conn.close()
                        del self.conns[addr[0] + ":" + str(addr[1])]
                        break
                    if decrypted_message == message_to_encrypt:
                        # Falls die Authorisierung erfolgreich ist, wird der Key neu gespeichert (und somit auch der
                        # Timestamp erneuert).
                        self.keys[remote_hash_key] = [addr[0], int(remote_port), serialized_remote_public_key,
                                                      datetime.timestamp(datetime.utcnow())]
                        response = "SUCCESS"
                    else:
                        print("distribute_name() : Peer " + addr[0] + ":" + remote_port + " (" + sending_peer_id +
                              ") konnte sich nicht korrekt identifizieren!")
                        response = "FAILURE"
            elif command == "QUERY":
                queried_position = int(msgsplit[1])
                if queried_position in list(self.keys):
                    # Falls der Key vorhanden ist, wird zunächst der Timestamp überprüft.
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
                # Wird ein Chat angefragt, starten wir einen neuen Thread, der sich mit dem anfragenden Peer verbindet.
                remote_ip = addr[0]
                remote_port = int(msgsplit[1])
                remote_name = msgsplit[2]
                chat_thread = threading.Thread(target=self.connect_chat, args=(remote_ip, remote_port, remote_name),
                                               daemon=True)
                chat_thread.start()
                conn.close()
                del self.conns[addr[0] + ":" + str(addr[1])]
                break
            elif command == "SHUTDOWN":
                # Wenn ein Peer einen gewollten Shutdown durchführt, sendet er uns entweder Informationen über seinen
                # Successor oder Predecessor (oder im 2-Peer-Fall auch beides)
                if msgsplit[1] == "SUCCESSOR:":
                    print("shutdown() : Setze neuen Successor: " + msgsplit[2] + ":" + msgsplit[3] + " (" + msgsplit[
                        4] + ")")
                    self.successor = [msgsplit[2], int(msgsplit[3]), int(msgsplit[4])]
                elif msgsplit[1] == "PREDECESSOR:":
                    print("shutdown() : Setze neuen Predecessor: " + msgsplit[2] + ":" + msgsplit[3] + " (" + msgsplit[
                        4] + ")")
                    self.predecessor = [msgsplit[2], int(msgsplit[3]), int(msgsplit[4])]
                # Zusätzlich können wir ihn direkt aus unserer Fingertabelle entfernen, sofern er sich darin befindet.
                if int(sending_peer_id) in list(self.fingers):
                    print("shutdown() : Finger " + sending_peer_id + " wird gelöscht.")
                    del self.fingers[int(sending_peer_id)]
            # Wenn eines der obigen Kommandos eine response verursacht, wird sie hier gesendet:
            if response != "":
                # print("Sending response: " + response)
                self.conns[addr[0] + ":" + str(addr[1])].send(bytes(response, "utf-8"))
