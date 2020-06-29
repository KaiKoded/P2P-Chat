# P2P Chat

## Notizen aus dem Meeting:

Authentifizierung zur Verhinderung von Identitätsdiebstahl

13. Juli Deadline

Keine feste portnummer hardcoden

server socket wie client socker dynamisch, damit man mit sich selbst testen kann

tupel aus IP und Port übergeben

viel loggen

netzverkehr aufzeichen (wireshark? PCP-Dump?) -> pcap datei

sockets parallel verarbeiten (parallele threads? -> Schwierig vor allem in Python)
oder Systemaufruf select (ohne parallelisierung)

SHA-2, MD5 hashfunktionen sind ok

keine Library die Sockets matched oder Chord implementiert
Library für DHT oder Verschlüsselung ok

1Thread pro Socket schlecht
SELECT in Python wartet auf mehrere threads bis man ein auf einem schreiben kann 
