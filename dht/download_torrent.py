# coding: u8

import atexit
import datetime
import json
import os
import os.path as osp
import string
import time
import urllib2

import psycopg2


root = osp.dirname(osp.abspath(__file__))
torrent_dir = osp.join(root, 'torrents')
if not osp.exists(torrent_dir):
    os.makedirs(torrent_dir)

conf = json.load(open(os.path.expanduser('~/.secret.json')))
pgconf = conf['dht']['pgsql']
conn = psycopg2.connect(database='dht', user=pgconf['user'],
        password=pgconf['pwd'], host=pgconf['host'], port=pgconf['port'])
cur = conn.cursor()


def exit():
    conn.commit()
    cur.close()
    conn.close()

atexit.register(exit)


def get_hashes(limit=20):
    cur.execute("""
    SELECT id, hash FROM hash WHERE hash.status = 0 limit %s;
    """ % limit)
    return cur.fetchall()


servers = [
    {
        'url' : 'https://zoink.it/torrent/%s.torrent',
        'case': string.upper
    },
    {
        'url' : 'http://torcache.net/torrent/%s.torrent',
        'case': string.upper
    },
    #{
    #    'url' : 'http://bt.box.n0808.com/%s/%s/%s.torrent',
    #    'case': lambda h: (h[:2], h[-2:], h)
    #},
    {
        'url' : 'http://torrage.com/torrent/%s.torrent',
        'case': string.upper
    },
]


def download(hash):
    h = hash.strip()
    for s in servers:
        print h, s['url'] % s['case'](h)
        try:
            res = urllib2.urlopen(s['url'] % s['case'](h), timeout=10).read()
        except KeyboardInterrupt:
            raise
        except:
            pass
        else:
            return res


def set_downloaded(id, status):
    print id, status
    cur.execute("""
    UPDATE hash SET status = %s WHERE id = %s
    """ % (status, id))


SUCCESS = 1
FAILURE = 2
while 1:
    time.sleep(.01)
    hs = get_hashes()
    if not hs:
        break
    for id, hash in hs:
        bt = download(hash)
        if bt is None:
            set_downloaded(id, FAILURE)
            continue
        set_downloaded(id, SUCCESS)
        open(osp.join(torrent_dir, hash) + '.torrent', 'wb').write(bt)

        now = datetime.datetime.now().strftime('%Y-%m-%d %H:%M:%S')
        s = now + '\t' + str(id) + '\n'
        open('/tmp/download_torrent.py.txt', 'a').write(s)
    conn.commit()
