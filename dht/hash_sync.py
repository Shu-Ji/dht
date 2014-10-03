# coding: u8

import atexit
import os
import os.path as osp
import urllib2

import psycopg2


class Sync(object):
    def __init__(self, pgconf):
        self.conn = psycopg2.connect(database='dht', user=pgconf['user'],
            password=pgconf['pwd'], host=pgconf['host'], port=pgconf['port']
        )

        self.servers = [
            'http://zoink.it/sync/',
            'http://torrage.com/sync/',
        ]

        self.cur = self.conn.cursor()

        atexit.register(self.exit)

        self.init_fetch()

    def init_fetch(self):
        import re
        p_re = re.compile(r'href="(\d{6}\.txt)"')
        for s in self.servers:
            print s
            html = urllib2.urlopen(s).read()
            paths = sorted([s + i for i in p_re.findall(html)])
            print paths
            log = 'sync-fetches-url.log'
            for p in paths:
                try:
                    if p in open(log).read():
                        continue
                except:
                    pass
                self.fetch_one(p)
                open(log, 'a').write(p + '\n')

    def fetch_one(self, url):
        print 'fetching: ', url

        path = osp.join('torrents/sync',
                url.split('//', 1)[-1].split('/', 1)[0])
        if not osp.exists(path):
            os.makedirs(path)

        path = osp.join(path, url.rstrip('/').rsplit('/', 1)[-1])
        if not osp.exists(path):
            html = urllib2.urlopen(url).read()
            open(path, 'w').write(html)
            print 'saving: ', path
        else:
            print 'exists.', path
            html = open(path).read()


        html = html.splitlines()

        print 'total: ', len(html)

        hashes = set()
        for h in html:
            h = h.strip().lower()
            if not h:
                continue
            hashes.add(h)
        if hashes:
            self.save(hashes)

    def exit(self):
        self.conn.commit()
        self.cur.close()
        self.conn.close()

    def save(self, hashes):
        hashes = list(hashes)

        per = 5000
        seq = [hashes[i:i+per] for i in range(0, len(hashes), per)]

        for hashes in seq:
            hashes = set(hashes)
            self.cur.execute("SELECT hash.hash FROM hash "\
                    "WHERE hash.hash IN %s " % str(tuple(hashes)))
            exist = {i[0] for i in self.cur.fetchall()}
            new = list(hashes - exist)
            print 'new: ', len(new)

            if not new:
                continue

            values = ','.join("('%s')" % i for i in new)

            self.cur.execute('''
            INSERT INTO hash(hash) VALUES %s
            ''' % values
            )
            self.conn.commit()


def main():
    import json
    import os
    conf = json.load(open(os.path.expanduser('~/.secret.json')))
    pgconf = conf['dht']['pgsql']

    Sync(pgconf)


if __name__ == '__main__':
    main()
