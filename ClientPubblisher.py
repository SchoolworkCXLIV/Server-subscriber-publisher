#!/usr/bin/python3
#
# ClientPubblisher.py - invia UN messaggio su un topic e termina
#
# Uso:  python3 ClientPubblisher.py <indirizzo_broker> <topic> <messaggio>
# Es.:  python3 ClientPubblisher.py 127.0.0.1 meteo "oggi c'e' il sole"
#       python3 ClientPubblisher.py 127.0.0.1:5000 meteo "ciao"

import socket
import sys

DEFAULT_PORT = 5000


def Main():
    if len(sys.argv) != 4:
        print(f"Uso: {sys.argv[0]} <indirizzo_broker> <topic> <messaggio>")
        sys.exit(1)

    address, topic, message = sys.argv[1], sys.argv[2], sys.argv[3]
    # l'indirizzo dell'utente puo' essere "host" oppure "host:porta"
    if ':' in address:
        host, port = address.split(':')
        port = int(port)
    else:
        host, port = address, DEFAULT_PORT

    # TODO: controllare che topic non contenga ':' o '\n' (romperebbe il protocollo)

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.sendall(f"pub:{topic}:{message}\n".encode('ascii'))
        sock.close()
        print("Messaggio inviato.")
    except OSError as e:
        print(f"Errore di connessione al broker: {e}")
        sys.exit(1)


if __name__ == '__main__':
    Main()
