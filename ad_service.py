# -*- coding: UTF-8 -*-

from whoosh.analysis import Tokenizer,Token
from whoosh.analysis import RegexTokenizer
from whoosh.index import create_in
from whoosh import index,sorting
from whoosh.qparser import QueryParser
from whoosh.fields import *
from radix_tree import RadixTree
import pymongo
from ac_trie import Trie
import json
import uuid
import config
import logging
from multiprocessing import Process, Queue
import time 
import re

logger = config.getLogger('ad_service',"./data/ad_service.log")

class update_task(Process):
    def __init__(self,task_queue,ix):
        Process.__init__(self)
        self.task_queue = task_queue
        self.ix = ix
        self.tagsParser = Trie(config.SKILL_FILE)
    def cut(self,value):
        value=value.lower().replace('&nbsp','')
        value = value.encode('UTF-8')
        terms = self.tagsParser.parse(value)
        v = {}
        for i in terms:
            v[i[0]]=i[1]
        return v.values()
    def update_doc(self,jdata):
        writer = self.ix.writer()
        for j in jdata['fields']:
            tags = self.cut(j['jobname']+' '+j['description'])
            jobid = j['jobid']
            orgid = j['orgid']
            jobname = unicode(j['jobname'])
            tags = ' '.join(tags).decode('UTF-8')
            ishunterjob=j['ishunterjob']
            
            writer.update_document(id=jobid,orgid=orgid,
                    title=jobname,tags=tags
                    ,ishunterjob=ishunterjob)
            logger.info('update doc :'+str(jobid))

        writer.commit()

    def del_doc(self,id):
        self.ix.delete_by_term('id',id)
        logger.info('del doc :'+str(id))

    def run(self):
        logger.info('启动异步任务队列')
        while(True):
            task = self.task_queue.get(1)
            if task[0] == 1:
                jdata = task[1]
                self.update_doc(jdata)
            elif task[0] == 2:
                id = task[1]
                self.del_doc(id)

class ADIndex:


    def __init__(self,ix,task_queue,pid):
        self.serverName = "%s_%d" %(config.get_mac_address(),pid)
        self.conn = pymongo.Connection(config.MONGO_CONN)
        self.pageurls = self.conn.pongo.pageurls
        self.task_queue = task_queue        
        self.ix = ix
        self.p = re.compile(u'(http://blog.csdn.net/[^\\/]+/article/details/\\d+)')
     
    def add_doc(self,jdata):
        self.task_queue.put((1,jdata))
        return {"message": "add doc ok"}
    def del_doc(self,id):
        self.task_queue.put((2,id))
        return {"del doc ":id}
    def find_by_query(self,q,limit):
        jobs = self.ix.searcher().search(q,limit=limit)
        return jobs
    def find_unique_orgid(self,q,limit):
        facet = sorting.FieldFacet("id", reverse=True)
        jobs = self.ix.searcher().search(q,collapse="orgid",sortedby=facet,limit=limit)
        return jobs
    def find_all(self,limit):
        qp = QueryParser("id", schema=self.ix.schema)
        q = qp.parse(u'*')
        return self.find_by_query(q,limit)
    def find_all_unique_orgid(self,limit):
        qp = QueryParser("id", schema=self.ix.schema)
        q = qp.parse(u'*')
        return self.find_unique_orgid(q,limit)
    def hunter_job(self,limit):
        qp = QueryParser("ishunterjob", schema=self.ix.schema)
        q = qp.parse(u'1')
        return self.find_unique_orgid(q,limit)

    def find(self,query,limit):
        query = query.strip()
        if len(query) == 0:
            query =u'*'
        searcher=self.ix.searcher()
        qp = QueryParser("tags", schema=self.ix.schema)
        q = qp.parse(query)
        return searcher.search(q,limit=limit)
#state 0  插入 链接
#state 1  插入 正文
#state 2  插入 标签
    def search_by_url(self,url,limit):
        pagetags = self.conn.pongo.pagetags
        url = unicode(url)
        m = self.p.search(url)
        if m :
            url = m.group(1)
            one = pagetags.find_one({"_id": url}, {"tags": 1})
            if one :
                tags = one["tags"]
                tags = tags.replace(',',' OR ')
                return self.find(tags,limit)
            else:
                self.insert_url(url)
                return None
        else:
            return None
    def insert_url(self,url):
        self.pageurls.insert({"_id":url})
        logger.info('adv insert :'+url)
    def jobs2json(self,jobs):
        rep = {}
        rep["server"] = self.serverName
        rep["state"] = True
        response = {}
        rep['response'] = response
        if jobs == None:
            response['totalCount']=0
            return rep
        response['totalCount']=len(jobs)
        #response['usedTime']=jobs.runtime
        items=[]
        for j in jobs:
            job={}
            job['jobId'] = j['id'] 
            job['orgid'] = j['orgid']
            job['jobTitle'] = j['title']
            items.append(job) 

        response['items']=items
        return rep
    def search(self,query,limit,hunterjob,uniqueorgid):
        if hunterjob:
            return self.hunter_job(limit)
        elif uniqueorgid:
            return self.find_all_unique_orgid(limit)
        else:
            return self.find(query,limit)


    def dispatch_hander(self,worker,frames):
        header = frames[2]
        data = frames[3]
        rep = ''
        try:
            jdata = json.loads(data.replace("''","0"),strict=False)
            action = jdata ["action"]
            rep = 'request err :'+data
            if action == "syncDoc":
                rep = {"message": "syncDoc ok"}
            elif header == 'update' and action == "updateDoc":
                rep = self.add_doc(jdata)
            #remove
            #{"action":"removeDoc","name":"job","keyID":"64983"}            
            elif header == 'remove' and action == "removeDoc":
                keyid = jdata ["keyID"]
                rep =self.del_doc(int(keyid))
            elif header == 'search':
                size = jdata['output']["size"]
                if action == 'adv':
                    referurl = jdata['q']["referurl"]
                    rep = self.jobs2json(self.search_by_url(referurl,size))
                    logger.info('adv:'+referurl)
                elif action == 'searchJob':
                    keyword = ''
                    uniqueorgid = False
                    hunterjob = False
                    if jdata.has_key('filter'):
                        f = jdata['filter']
                        if f.has_key('uniqueKey'):
                            uniqueorgid = True
                        if f.has_key('jobflag'):
                            hunterjob = True
                    if jdata.has_key('q') and jdata['q'].has_key('keyword'):
                        keyword = jdata['q']["keyword"]
                    rep = self.jobs2json(self.search(keyword,size,hunterjob,uniqueorgid))
                    logger.info('searchJob:keyword['+keyword+']')
                elif action == 'all' :#所有职位
                    rep = self.jobs2json(self.find_all(size))
                    logger.info('search all')
                elif action == 'uniqueorgid': #按orgid排重后的所有职位
                    rep = self.jobs2json(self.find_all_unique_orgid(size))
                    logger.info('search uniqueorgid')
                elif action == 'hunterjob':#获取最新猎头数据
                    rep = self.jobs2json(self.hunter_job(size))
                    logger.info('search hunterjob')
        except Exception,e:
            logger.error("except:%s\n%s" %(frames,e))

        rep = json.dumps(rep)
        msg = [frames[0],frames[1],rep.encode('UTF-8')]
        if len(frames) == 5 :
            msg.append(frames[4])

        worker.send_multipart(msg)

if __name__ == '__main__' :
    aix = ADIndex('indexdir')
    jobs =[
            (1,u'java搜索',u'java linux lucene'),
            (2,u'ruby开发',u'java linux linux ruby'),
            (3,u'python开发',u'java linux linux python')
          ]
    aix.add_doc(jobs)
    aix.del_doc(2)
    jobs = aix.find(u'java linux',10,False,False)
    print jobs
    for j in jobs:
        print j,j.score
