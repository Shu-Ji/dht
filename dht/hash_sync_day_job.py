# coding: u8

import atexit
import json
import logging
import os

from apscheduler.schedulers.blocking import BlockingScheduler as Scheduler

import psycopg2


sched = Scheduler(standalone=True, daemonic=False)
pgconf = json.load(open(os.path.expanduser('~/.secret.json')))['dht']['pgsql']


def get_db():
    conn = psycopg2.connect(database='dht', user=pgconf['user'],
        password=pgconf['pwd'], host=pgconf['host'], port=pgconf['port']
    )
    cur = conn.cursor()
    return conn, cur


@sched.scheduled_job(trigger='cron', hour=13, minute=30)
def hash_sync():
    logger = logging.getLogger('hash_sync')

    fh = logging.FileHandler('log/hash_sync.log')
    formatter = logging.Formatter('%(asctime)s %(message)s',
            '%Y-%m-%d %H:%M:%S')
    fh.setFormatter(formatter)

    logger.addHandler(fh)
    logger.setLevel(logging.INFO)

    def save(hashes):
        hashes = list(hashes)
        per = 5000
        seq = [hashes[i:i+per] for i in range(0, len(hashes), per)]

        for hashes in seq:
            hashes = set(hashes)
            cur.execute("SELECT hash.hash FROM hash "\
                    "WHERE hash.hash IN %s " % str(tuple(hashes)))
            exist = {i[0] for i in cur.fetchall()}
            new = list(hashes - exist)
            logger.info('new: ' + str(len(new)))

            if not new:
                continue

            values = ','.join("('%s')" % i for i in new)

            cur.execute('''
            INSERT INTO hash(hash) VALUES %s
            ''' % values
            )
            conn.commit()

    import requests
    import datetime

    today = datetime.datetime.now().strftime('%Y%m%d')
    conn, cur = get_db()

    servers = [
        'http://zoink.it/sync/',
        'http://torrage.com/sync/',
    ]

    for s in servers:
        url = s + today + '.txt'
        logger.info(url)
        try:
            html = requests.get(url).content.splitlines()
        except:
            logger.error('http get failed.')
            continue

        logger.info('total: ' + str(len(html)))

        hashes = set()
        for h in html:
            h = h.strip().lower()
            if not h:
                continue
            hashes.add(h)
        if hashes:
            save(hashes)

    logger.info('over')


atexit.register(lambda: sched.shutdown(wait=False))


if __name__ == '__main__':
    sched.start()
