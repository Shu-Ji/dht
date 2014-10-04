# -*- coding: UTF-8 -*-
from whoosh.index import open_dir
from whoosh.qparser import QueryParser

import os.path as osp

import jieba
from jieba.analyse.analyzer import STOP_WORDS, accepted_chars

from whoosh.analysis import (LowercaseFilter, StopFilter, StemFilter)
from whoosh.analysis import Tokenizer, Token
from whoosh.lang.porter import stem

dht_whoosh_dir = osp.expanduser('~/dht_whoosh_store')

punct = set(u''':!),.:;?]}¢'"、。〉》」』】〕〗〞︰︱︳﹐､﹒
﹔﹕﹖﹗﹚﹜﹞！），．：；？｜｝︴︶︸︺︼︾﹀﹂﹄﹏､～￠
々‖•·ˇˉ―--′’”([{£¥'"‵〈《「『【〔〖（［｛￡￥〝︵︷︹︻
︽︿﹁﹃﹙﹛﹝（｛“‘-—_…''')
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


ix = open_dir(dht_whoosh_dir) # for read only
searcher = ix.searcher()
parser = QueryParser('name', schema=ix.schema)

for keyword in (u'hello',):
    q = parser.parse(keyword)
    results = searcher.search(q)
    print 'result of ', keyword
    for hit in results:
        print hit['hash'], hit['name']
    print '=' * 10
