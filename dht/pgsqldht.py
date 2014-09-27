# coding: u8

import atexit
import dhtlib
import psycopg2


class Store(dhtlib.Store):
    def __init__(self, db='dht', user='postgres', pwd='123', host='127.0.0.1',
            port=5432, max_cache_size=500):
        self.conn = psycopg2.connect(database=db, user=user, password=pwd,
            host=host, port=port,
        )
        self.cur = self.conn.cursor()
        dhtlib.Store.__init__(self)

        self.create_table()

        atexit.register(self.exit)

        self.cache = set()

    def create_table(self):
        self.cur.execute('''
        CREATE TABLE IF NOT EXISTS hash(
            id serial PRIMARY KEY,
            hash varchar(40)
        );
        ''')
        self.conn.commit()

    def exit(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def save(self, infohash):
        print infohash
        if not infohash in self.cache:
            self.cache.add(infohash)
            if len(list(self.cache)) > self.max_cache_size:
                print 'flushing...'
                tmp, self.cache = self.cache, set()
                values = ','.join('(%s)' % i for i in tmp)
                self.cur.execute('''
                INSERT INTO hash(hash) VALUES %s
                ''' % values
                )
                self.conn.commit()


def main():
    dhtlib.Server(store=Store()).foreverloop()


if __name__ == '__main__':
    main()
