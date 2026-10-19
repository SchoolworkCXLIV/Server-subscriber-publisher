#!/usr/bin/python3
#
# ClientSubscriber.py - si sottoscrive a un topic e stampa i messaggi ricevuti
#
# Uso:  python3 ClientSubscriber.py <indirizzo_broker> <topic>
# Es.:  python3 ClientSubscriber.py 127.0.0.1 meteo

import socket
import sys

DEFAULT_PORT = 5000


def readLine(sk):
    # Legge dal socket fino al '\n'. Ritorna b'' se il broker chiude la connessione.
    data = b''
    while b'\n' not in data:
        packet = sk.recv(1)
        if not packet:
            return b''
        data += packet
    return data.strip()


def Main():
    if len(sys.argv) != 3:
        print(f"Uso: {sys.argv[0]} <indirizzo_broker> <topic>")
        sys.exit(1)

    address, topic = sys.argv[1], sys.argv[2]

    if ':' in address:
        host, port = address.split(':')
        port = int(port)
    else:
        host, port = address, DEFAULT_PORT

    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.connect((host, port))
        sock.sendall(f"sub:{topic}\n".encode('ascii'))
        print(f"Sottoscritto al topic '{topic}'. Ctrl+C per uscire.")

        while True:
            line = readLine(sock)
            if not line:
                print("Il broker ha chiuso la connessione.")
                break
            print(line.decode('ascii', errors='replace'))
    except KeyboardInterrupt:
        print("\nUscita.")
    except OSError as e:
        print(f"Errore di connessione al broker: {e}")
    finally:
        sock.close()


if __name__ == '__main__':
    Main()
