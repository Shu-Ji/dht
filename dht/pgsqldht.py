# coding: u8

import atexit
import datetime

import psycopg2

import dhtlib


class Store(dhtlib.Store):
    def __init__(self, user, pwd, host, port, db='dht', max_cache_size=1500):
        self.max_cache_size = max_cache_size

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
            id SERIAL NOT NULL,
            hash CHAR(40) NOT NULL UNIQUE,
            status INT2 NOT NULL DEFAULT 0
        );
        -- CREATE INDEX status_index ON hash(status);
        ''')
        self.conn.commit()

    def exit(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def save(self, infohash):
        self.cache.add(infohash)
        if len(list(self.cache)) > self.max_cache_size:
            cache, self.cache = self.cache, set()

            self.cur.execute("SELECT hash.hash FROM hash "\
                    "WHERE hash.hash IN %s " % str(tuple(cache)))
            exist = {i[0] for i in self.cur.fetchall()}
            new = list(cache - exist)

            if not new:
                return

            values = ','.join("('%s')" % i for i in new)
            self.cur.execute('''
            INSERT INTO hash(hash) VALUES %s
            ''' % values
            )
            now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
            cnt = str(len(new))
            s = now + '\t' + cnt + '\n'
            open('/tmp/pgsqldht.py.infohash.txt', 'a').write(s)
            self.conn.commit()


def main():
    import json
    import os
    conf = json.load(open(os.path.expanduser('~/.secret.json')))
    pgconf = conf['dht']['pgsql']
    dhtlib.Server(store=Store(user=pgconf['user'], pwd=pgconf['pwd'],
        host=pgconf['host'], port=pgconf['port'])).foreverloop()


if __name__ == '__main__':
    main()
