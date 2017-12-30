#coding:utf-8
from elasticsearch2 import Elasticsearch
from datetime import datetime

es = Elasticsearch(hosts="10.10.6.6")

es.index(index="keti10_10", doc_type="keti10_10", id=3, body={"bdcdyh": "123", "lx": '1',\
 'postDate':'2017-12-30 12:11:06','qx':'北京','records':2,'uuid':'00123dfad','zl':'北京海淀区'})

#doc=es.get(index="keti10_10", doc_type="keti10_10", id=1)['_source']

#print "doc is %s" % doc

res = es.search(index="keti10_10", body={"query": {"match_phrase": {"zl":'北京'} }})

for hit in res['hits']['hits']:
  hitmap=hit['_source']
  print "%(zl)s %(postDate)s"  %  hitmap

