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
def debug():
    print '=====================================>添加文档'
    request = '{"action":"updateDoc","name":"job","keyID":65072,"fields":[{"jobid":65072,"jobsnapid":74813,"jobname":"北京急聘高级PHP工程师","degreecode":2,"jobtypecode":1,"orgid":358813,"orgname":"上海爱极企业管理咨询有限公司","orgflag":0,"workexperience":4,"description":"\r\n\tPHP资深开发工程师\r\n\r\n\t工作职责：\r\n\t1、编写相关开发文档，并能独立或带领其他同事完成相关工作任务；\r\n\t2、配合运维等相关同事，完成架构建设及优化；\r\n\r\n\t职位需求\r\n\r\n\t1、有2年以上PHP编程开发经验，熟悉面向对象的软件设计方法，\r\n\r\n\t熟练掌握XHTML、CSS、DIV、Javascript等页面技术；\r\n\r\n\t熟悉流行PHP开源系统二次开发，如discuz、，能快速整合开源社区、论坛、shop等功能，并进行个性化的修改;\r\n\r\n\t&nbsp;\r\n\r\n\t2、熟悉Mysql数据库应用开发，具备数据库的设计及优化能力；\r\n\r\n\t&nbsp;\r\n\r\n\t3、熟悉Unix/Linux操作系统及常用命令，熟悉Shell脚本编程；\r\n\r\n\t&nbsp;\r\n\r\n\t4、具备良好的沟通能力；具备良好的代码编程习惯及较强的文档编写能力；\r\n\r\n\t&nbsp;\r\n\r\n\t5、具备团队合作精神，有良好的沟通及协调能力；\r\n\r\n\t&nbsp;\r\n\r\n\t6、熟悉开源的B2C商城系统，深入了解其内部机制，有过成熟的二次开发经验者尤佳\r\n\r\n\t北京互联网公司，近200人， 职位月薪在10-15K 之间，13薪， 欢迎咨询","salaryrangecode":1300,"PublishTime":"2013-05-31T17:29:39.2","refreshtime":"2013-06-14T11:09:06.227","orderdate":"2013-06-14T11:09:06.227","companytype":0,"companymemsize":0,"jobaddress":"110000#110100","businesstags":"4003#游戏（网游、页游、TVgame）,0#B2C电商","jobcategorys":"1004","ishunterjob":3,"score":100,"ownersys":0,"skillsmap":[]}],"output":{"format":"json","offset":0,"size":10}}'
    print send_request('update',request)


    print '=====================================>广告匹配'
    url = 'http://blog.csdn.net/0210/article/details/4458407'
    request='{"action":"adv","q":{"referurl":"'+url.strip()+'","keyword":[""]},"filter":{"city":[""],"province":[""]},"sort":1,"output":{"format":"json","offset":0,"size":30}}'
    req2json(request)


    print '=====================================>查找所有'
    request='{ "action" : "all" , "sort" : 1 , "output" : { "format" : "json" , "offset" : 0 , "size" : 30}}'
    req2json(request)

    print '=====================================>猎头职位'
    request='{ "action" : "hunterjob" , "sort" : 1 , "output" : { "format" : "json" , "offset" : 0 , "size" : 30}}'
    req2json(request)

    print '=====================================>每家公司各一条'
    request='{ "action" : "uniqueorgid" , "sort" : 1 , "output" : { "format" : "json" , "offset" : 0 , "size" : 30}}'
    req2json(request)

    k = 'php'
    print '=====================================>查询关键词',k
    request='{ "action" : "searchJob" , "q" : { "keyword" : "'+k+'"} , "sort" : 1 , "output" : { "format" : "json" , "offset" : 0 , "size" : 30}}'
    req2json(request)

    print '=====================================>删除索引',65072
    request = '{"action":"removeDoc","name":"job","keyID":"65072"}'
    print send_request('remove',request)

    print '=====================================>清除缓存'
    request = '{"action":"cacheclean"}'
    print send_request('cacheclean',request)

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
    elif action == 'key':
            k = 'java'
            request='{ "action" : "searchJob" , "q" : { "keyword" : "'+k+'"} , "sort" : 1 , "output" : { "format" : "json" , "offset" : 0 , "size" : 30}}'
            print k
            req2json(request)
    elif action == 'debug':
        debug()
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



