#### Projektgruppe 20

Kai Ketzler (608273)

Alpay Yilmaz (566833)

Adrian Schimmelpfennig (573700)


#### Benötigte Packages

cryptography

appJar

numpy

(+ dependencies)

-> requirements.txt


#### Anleitung

1.) App.py ausführen

2.) Username, eigenen Listen-Port und Point of Entry eingeben

- Für Testzwecke kann der Point of Entry auch einfach nur ein Port sein, dann wird automatisch localhost eingesetzt.
	
- Aufpassen, dass zwischen den Ports einzelner lokaler Peers mindestens ein Abstand von 2 besteht, weil für den Chat auf (Chord-Port + 1) zugehört wird
	
- Für Peer 0 muss der Point of Entry leer gelassen werden. So setzt man ein neues Netzwerk auf.


#### Anmerkungen

- Ein Klick auf das X führt einen regulären Shutdown durch
- Darauf achten, dass alle Chats geschlossen sind, bevor man die Anwendung schließt, sonst meckert appJar
- Es werden im Verzeichnis Files für den public- und private key generiert. Um den Authentifizierungsmechanismus lokal zu testen, muss man diese löschen, bevor man einen neuen Peer hinzufügt, der einen bereits vergebenen Namen annehmen soll (das Löschen beeinflusst die bereits vorhandenen Peers nicht)
- In Settings.py kann man u.A. die Lebensdauer der Key-Value-Pairs einstellen
