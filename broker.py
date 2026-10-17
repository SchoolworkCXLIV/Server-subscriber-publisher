#!/usr/bin/python3
#
# broker.py - server TCP publisher/subscriber
#
# Protocollo (ogni messaggio termina con '\n'):
#   sub:topic            -> il client si sottoscrive a "topic"
#   pub:topic:message    -> il client pubblica "message" su "topic"
# Il broker inoltra ai subscriber la riga:  topic:message\n

import threading
import socket
import logging

HOST = ''
PORT = 5000

logging.basicConfig(
    level=logging.DEBUG,
    format='%(asctime)s [%(levelname)s] %(threadName)s: %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S'
)

# Stato condiviso tra i thread: { "topic": [socket_sub1, socket_sub2, ...] }
# Poiche' piu' thread lo modificano insieme, ogni accesso va protetto con il lock.
subscribers = {}
subscribers_lock = threading.Lock()


def readLine(sk):
    """Legge dal socket fino al '\\n'. Ritorna b'' se il client si disconnette."""
    data = b''
    while b'\n' not in data:
        packet = sk.recv(1)
        if not packet:
            return b''
        data += packet
    return data.strip()


def addSubscriber(topic, sock):
    with subscribers_lock:
        subscribers.setdefault(topic, []).append(sock)
    logging.info(f"nuova sottoscrizione al topic '{topic}'")


def removeSubscriber(sock):
    """Toglie il socket da tutti i topic (chiamata quando il client si disconnette)."""
    with subscribers_lock:
        for topic in list(subscribers):
            if sock in subscribers[topic]:
                subscribers[topic].remove(sock)
            if not subscribers[topic]:
                del subscribers[topic]   # nessun iscritto: elimino il topic


def publish(topic, message):
    """Inoltra il messaggio a tutti i subscriber del topic."""
    line = f"{topic}:{message}\n".encode('ascii', errors='replace')
    with subscribers_lock:
        targets = list(subscribers.get(topic, []))   # copia: invio fuori dal lock
    for s in targets:
        try:
            s.sendall(line)
        except OSError:
            logging.warning("subscriber non raggiungibile, lo rimuovo")
            removeSubscriber(s)
    logging.info(f"messaggio su '{topic}' inoltrato a {len(targets)} subscriber")


class ClientThread(threading.Thread):
    """Un thread per ogni client connesso (publisher o subscriber)."""

    def __init__(self, sock, addr):
        threading.Thread.__init__(self, name=f"client-{addr[0]}:{addr[1]}")
        self.sock = sock
        self.addr = addr

    def run(self):
        logging.info("client connesso")
        while True:
            line = readLine(self.sock)
            if not line:
                break   # client disconnesso

            text = line.decode('ascii', errors='replace')
            logging.debug(f"ricevuto: {text}")

            # maxsplit=2: il messaggio puo' contenere dei ':' senza essere spezzato
            parts = text.split(':', 2)
            cmd = parts[0]

            if cmd == 'sub' and len(parts) == 2 and parts[1]:
                addSubscriber(parts[1], self.sock)
            elif cmd == 'pub' and len(parts) == 3 and parts[1]:
                publish(parts[1], parts[2])
            else:
                logging.warning(f"comando non valido: {text}")
                # TODO: rispondere al client con un messaggio di errore (es. 'ERR\n')

        removeSubscriber(self.sock)
        self.sock.close()
        logging.info("client disconnesso")


def Main():
    try:
        s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        s.bind((HOST, PORT))
        s.listen(5)
        logging.info(f"broker in ascolto sulla porta {PORT}")
    except Exception as e:
        logging.critical(f"impossibile avviare il broker: {e}")
        return

    while True:
        try:
            c, addr = s.accept()
            ClientThread(c, addr).start()
        except KeyboardInterrupt:
            logging.info("chiusura del broker (Ctrl+C)")
            break
        except Exception as e:
            logging.error(f"errore: {e}")

    s.close()


if __name__ == '__main__':
    Main()
