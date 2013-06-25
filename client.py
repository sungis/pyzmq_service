#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
#=============================================================================
#     FileName: client.py
#         Desc: 客户端
#       Author: Sungis
#        Email: mr.sungis@gmail.com
#     HomePage: http://sungis.github.com
#      Version: 0.0.1
#   LastChange: 2013-06-21 19:49:52
#      History:
#=============================================================================
'''
import zmq  
import re
import sys
import json

SERVER_ENDPOINT = "tcp://localhost:5555"
#SERVER_ENDPOINT = "tcp://10.0.1.77:5555"
CLIENT_IDENTITY = "AD_Client_pyzmq" 

def send_request(header,q):
  c=zmq.Context()
  s=c.socket(zmq.REQ)
#  s.setsockopt(zmq.IDENTITY,CLIENT_IDENTITY)
  s.connect(SERVER_ENDPOINT)
  m=[header,q]
  s.send_multipart(m)
  return s.recv()

import threading
from time import sleep,ctime

url ='http://blog.csdn.net/grhunter/article/details/1203694'
request='{"action":"adv","q":{"referurl":"'+url+'","keyword":[""]},"filter":{"city":[""],"province":[""]},"sort":1,"output":{"format":"json","offset":0,"size":30}}'
m=['search',request]
class advClient(threading.Thread):
    def __init__(self,id):
        threading.Thread.__init__(self)
        c=zmq.Context()
        self.s=c.socket(zmq.REQ)
        self.s.setsockopt(zmq.IDENTITY,CLIENT_IDENTITY+'__'+str(id))
        self.s.connect(SERVER_ENDPOINT)
    def run(self):
        for i in range(100):
            self.s.send_multipart(m)
            rep = self.s.recv()
            #print i

def req2json(request):
    data = send_request('search',request)
    jdata = json.loads(data)
    print jdata["server"],'totalCount',jdata["response"]['totalCount']
    if jdata["response"]['totalCount']>0:
        jobs = jdata["response"]["items"]
        for i in jobs:
            print i["jobTitle"]

if __name__  == '__main__':
    action = sys.argv[1]
    if action == 'update':
        f = open('./data/jobs.txt')
        re_jobs=re.compile('{"action":"updateDoc".*{"format":"json","offset":0,"size":10}}')
        for i in f:
            m = re_jobs.search(i)
            if m:
                request = m.group()
                print send_request('update',request)

    elif action == 'remove' :
        request = '{"action":"removeDoc","name":"job","keyID":"67943"}'
        print send_request('remove',request)

    elif action == 'cacheclean' :
        request = '{"action":"cacheclean"}'
        print send_request('cacheclean',request)

    elif action == 'adv':
        while True:
            print 'input url ==>'
            url = sys.stdin.readline()
            request='{"action":"adv","q":{"referurl":"'+url.strip()+'","keyword":[""]},"filter":{"city":[""],"province":[""]},"sort":1,"output":{"format":"json","offset":0,"size":30}}'
            req2json(request)

    elif action=='all':
        request='{ "action" : "all" , "sort" : 1 , "output" : { "format" : "json" , "offset" : 0 , "size" : 30}}'
        req2json(request)

    elif action=='hunterjob':
        request='{ "action" : "hunterjob" , "sort" : 1 , "output" : { "format" : "json" , "offset" : 0 , "size" : 30}}'
        req2json(request)

    elif action=='uniqueorgid':
        request='{ "action" : "uniqueorgid" , "sort" : 1 , "output" : { "format" : "json" , "offset" : 0 , "size" : 30}}'
        req2json(request)

    elif action=='test':
        while(True):
            print 'input:'
            request = sys.stdin.readline()
            print request
            req2json(request)
    elif action == 'thread':
        threads=[]
        n=100
        for i in range(n):
            t=advClient(i)
            threads.append(t)
            t.start()
        for i in range(n):
            threads[i].join()

    else:
        f = open('skill.txt')
        for k in f:
            request='{ "action" : "searchJob" , "q" : { "keyword" : "'+k+'"} , "sort" : 1 , "output" : { "format" : "json" , "offset" : 0 , "size" : 30}}'
            print k
            print send_request('search',request)
            ch = sys.stdin.read(1)



