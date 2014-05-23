# coding: u8

import atexit
import dhtlib
import pymongo


class Store(dhtlib.Store):
    def __init__(self, host='127.0.0.1', port=27017, max_cache_size=500):
        client = pymongo.MongoClient(host=host, port=port)
        self.table = client.infohashdb.infohash

        self.table.ensure_index([('h', pymongo.ASCENDING)])
        self.table.ensure_index([('t', pymongo.DESCENDING)])

        import datetime
        self.now = datetime.datetime.now

        self.max_cache_size = max_cache_size
        self.cache = []

        atexit.register(self.flush)

        dhtlib.Store.__init__(self)

    def save(self, infohash):
        if self.table.find_one({'h': infohash}) is not None:
            return
        self.cache.append(infohash)
        if len(self.cache) > self.max_cache_size:
            print 'flushing...'
            self.tmp = self.cache[:]
            self.cache = []
            self.flush()

    def flush(self):
        hashes = [{'h': h, 't': self.now()} for h in self.tmp]
        self.table.insert(hashes)


def main():
    dhtlib.Server(store=Store()).foreverloop()


if __name__ == '__main__':
    main()
