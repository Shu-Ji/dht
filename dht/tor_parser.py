# coding: u8

import atexit
import datetime
import glob
import gzip
import json
import os
import os.path as osp
import shutil

import psycopg2
from psycopg2.extras import Json

import bencode
import jieba
from jieba.analyse.analyzer import STOP_WORDS, accepted_chars

from whoosh.index import create_in, open_dir, exists_in
from whoosh.fields import TEXT, Schema, STORED
from whoosh.analysis import (LowercaseFilter, StopFilter, StemFilter)
from whoosh.analysis import Tokenizer, Token
from whoosh.lang.porter import stem


class ChineseTokenizer(Tokenizer):
    def __call__(self, text, **kargs):
        token  = Token()

        words = set()
        words_list = []

        for (i, start_pos, stop_pos) in jieba.tokenize(text, mode='search'):
            i = i.strip()
            if not i:
                continue
            if i in words:
                continue
            if i in punct:
                continue
            words.add(i)
            words_list.append(i)

        for w in words:
            if not accepted_chars.match(w):
                if len(w) <= 1:
                    continue
            token.original = token.text = w
            token.pos = start_pos
            token.startchar = start_pos
            token.endchar = stop_pos
            yield token


def ChineseAnalyzer(stoplist=STOP_WORDS, minsize=1,
        stemfn=stem, cachesize=50000):
    return ChineseTokenizer() | LowercaseFilter() |\
            StopFilter(stoplist=stoplist, minsize=minsize) |\
            StemFilter(stemfn=stemfn, ignore=None, cachesize=cachesize)


analyzer = ChineseAnalyzer()

conf = json.load(open(os.path.expanduser('~/.secret.json')))
pgconf = conf['dht']['pgsql']
conn = psycopg2.connect(database='dht', user=pgconf['user'],
        password=pgconf['pwd'], host=pgconf['host'], port=pgconf['port'])
cur = conn.cursor(cursor_factory=psycopg2.extras.NamedTupleCursor)


def exit():
    conn.commit()
    cur.close()
    conn.close()

atexit.register(exit)

punct = set(u''':!),.:;?]}¢'"、。〉》」』】〕〗〞︰︱︳﹐､﹒
﹔﹕﹖﹗﹚﹜﹞！），．：；？｜｝︴︶︸︺︼︾﹀﹂﹄﹏､～￠
々‖•·ˇˉ―--′’”([{£¥'"‵〈《「『【〔〖（［｛￡￥〝︵︷︹︻
︽︿﹁﹃﹙﹛﹝（｛“‘-—_…''')

dht_whoosh_dir = osp.expanduser('~/dht_whoosh_store')
if not osp.exists(dht_whoosh_dir):
    os.makedirs(dht_whoosh_dir)

if not exists_in(dht_whoosh_dir):
    schema = Schema(hash=STORED, name=TEXT(stored=True, analyzer=analyzer))
    ix = create_in(dht_whoosh_dir, schema)
else:
    ix = open_dir(dht_whoosh_dir)


def set_status(hash, status=4, no_sql=False):
    if not no_sql:
        cur.execute('''
        UPDATE hash SET status = %s WHERE hash.hash = '%s'
        ''' % (status, hash))
        conn.commit()

    i = osp.join(hash[:2], hash[-2:])
    path =  osp.join(osp.expanduser('~/torrents.bak'), i, str(status))
    if not osp.exists(path):
        os.makedirs(path)

    i = osp.join('torrents', i, '%s.torrent' % hash)
    shutil.move(i, osp.join(path, hash + '.torrent'))


def run():
    idx = 0
    writer = ix.writer()
    for i in glob.iglob('torrents/*/*/*.torrent'):
        idx += 1

        hash = i.rsplit('/', 1)[-1].split('.')[0]

        try:
            raw_data = gzip.open(i).read()
        except:
            set_status(hash, 6)
            continue

        try:
            data = bencode.bdecode(raw_data)
        except:
            set_status(hash, 4)
            continue

        info = data.get('info', {})

        if not info:
            set_status(hash, 4)
            continue

        dt = data.get('creation date', 315504000)
        try:
            dt = datetime.datetime.fromtimestamp(dt)
        except:
            dt = datetime.date(1980, 1, 1)
        result = {'creation_date': dt}

        files = info.get('files', [])
        encoding = data.get('encoding', 'u8')

        if not files:
            length = info.get('length', 0)
        else:
            length = 0
            fs = []
            try:
                for f in files:
                    l = f.get('length', 0)
                    length += l

                    for i, p in enumerate(f['path']):
                        f['path'][i] = p.decode(encoding)

                    fs.append(['/'.join(f['path']), l])
            except:
                set_status(hash, 5)
                continue
            files = fs

        name = info.get('name', '')

        try:
            name = name.decode(encoding)
        except:
            set_status(hash, 5)
            continue

        writer.add_document(hash=hash, name=name)

        result['hash'] = hash
        result['length'] = length
        result['files'] = Json({'fs': files})
        result['name'] = name.encode('u8')

        #print encoding, hash, name

        cur.execute('''
        UPDATE hash SET
        status = 3,
        name = %(name)s,
        length = %(length)s,
        files = %(files)s,
        creation_date = %(creation_date)s
        WHERE hash.hash = %(hash)s
        ''', result)

        print idx,
        if idx % 20 == 0:
            print
        writer.commit()
        writer = ix.writer()
        conn.commit()
        set_status(hash, 3, no_sql=True)


if __name__ == '__main__':
    run()
    print 'over'
