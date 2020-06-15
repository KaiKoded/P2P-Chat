# P2P Chat

## Notizen aus dem Meeting:

Authentifizierung zur Verhinderung von Identitätsdiebstahl

13. Juli Deadline

Keine feste portnummer hardcoden
server socket wie client socker dynamisch, damit man mit sich selbst testen kann

tupel aus IP und Tupel übergeben

viel loggen

netzverkehr aufzeichen (wireshark?) -> pcap datei

sockets parallel verarbeiten (parallele threads? -> Schwierig vor allem in Python)
oder Systemaufruf select (ohne parallelisierung)

SHA-2, MD5 hashfunktionen sind ok
