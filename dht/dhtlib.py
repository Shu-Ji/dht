# coding: u8

import hashlib
import random
import socket
import struct
from threading import Timer, Thread, RLock
import time

from bencode import bencode, bdecode


BOOTSTRAP_NODES = [
    ("router.bittorrent.com", 6881),
    ("dht.transmissionbt.com", 6881),
    ("router.utorrent.com", 6881),
]
TID_LENGTH = 4
KRPC_TIMEOUT = 10


def entropy(bytes):
    return ''.join([chr(random.randint(0, 255)) for i in range(bytes)])


def random_id():
    hash = hashlib.sha1()
    hash.update(entropy(20))
    return hash.digest()


def decode_nodes(nodes):
    n = []
    length = len(nodes)
    if (length % 26) != 0:
        return n
    for i in range(0, length, 26):
        nid = nodes[i:i+20]
        ip = socket.inet_ntoa(nodes[i+20:i+24])
        port = struct.unpack("!H", nodes[i+24:i+26])[0]
        n.append((nid, ip, port))
    return n


def timer(t, f):
    Timer(t, f).start()


class KRPC(Thread):
    def __init__(self):
        Thread.__init__(self)
        self.setDaemon(True)
        self.join_successed = False
        self.types = {
            "r": self.response_received,
            "q": self.query_received
        }
        self.actions = {
            "get_peers": self.get_peers_received,
        }

        self.ufd = socket.socket(socket.AF_INET,
            socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        self.ufd.bind((self.ip, self.port))

    def response_received(self, msg, address):
        try:
            self.join_successed = True
            nodes = decode_nodes(msg["r"]["nodes"])
        except KeyError:
            pass
        else:
            for node in nodes:
                nid, ip, port = node
                if len(nid) != 20 or ip == self.ip:
                    continue
                self.table.put(KNode(nid, ip, port))

    def query_received(self, msg, address):
        try:
            self.actions[msg["q"]](msg, address)
        except KeyError:
            pass

    def send_krpc(self, msg, address):
        try:
            self.ufd.sendto(bencode(msg), address)
        except:
            pass

    def get_neighbor(self, target):
        return target[:10] + random_id()[10:]


class Client(KRPC):
    def __init__(self, table):
        self.table = table
        self.lock = RLock()

        timer(KRPC_TIMEOUT, self.timeout)
        KRPC.__init__(self)

    def find_node(self, address, nid=None):
        nid = self.get_neighbor(nid) if nid else self.table.nid
        tid = entropy(TID_LENGTH)
        msg = {
            "t": tid,
            "y": "q",
            "q": "find_node",
            "a": {"id": nid, "target": random_id()}
        }
        self.send_krpc(msg, address)

    def bootstrap(self):
        for address in BOOTSTRAP_NODES:
            self.find_node(address)

    def timeout(self):
        if not self.join_successed:
            self.bootstrap()
        timer(KRPC_TIMEOUT, self.timeout)

    def run(self):
        self.bootstrap()
        while 1:
            time.sleep(.001)
            try:
                data, address = self.ufd.recvfrom(65536)
                msg = bdecode(data)
                self.types[msg["y"]](msg, address)
            except Exception:
                pass

    def foreverloop(self):
        self.start()
        while 1:
            time.sleep(.001)
            if not self.table.nodes:
                self.join_successed = False
                time.sleep(1)
                continue

            for node in self.table.nodes:
                self.find_node((node.ip, node.port), node.nid)

            self.lock.acquire()
            self.table.nodes = []
            self.lock.release()


class Server(Client):
    def __init__(self, ip='0.0.0.0', port=6881, max_node_qsize=100,
            store=None):
        self.max_node_qsize = max_node_qsize
        self.table = KTable(max_node_qsize)
        self.ip = ip
        self.port = port
        self.store = store or Store()

        Client.__init__(self, self.table)

    def get_peers_received(self, msg, address):
        try:
            infohash = msg["a"]["info_hash"]
            infohash = infohash.encode("hex")
            self.store.save(infohash)
        except Exception:
            pass


class KTable():
    def __init__(self, max_node_qsize):
        self.max_node_qsize = max_node_qsize
        self.nid = random_id()
        self.nodes = []

    def put(self, node):
        if len(self.nodes) <= self.max_node_qsize:
            self.nodes.append(node)


class KNode(object):
    def __init__(self, nid, ip=None, port=None):
        self.nid = nid
        self.ip = ip
        self.port = port


class Store(object):
    def save(self, infohash):
        print infohash


if __name__ == "__main__":
    Server().foreverloop()
