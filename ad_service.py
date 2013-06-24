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
from cache.lrucache import LRUCache
from hashlib import md5
import config
import logging
from Queue import Queue
import threading 
import time 

LOG_FILENAME="./data/ad_service.log"
logger=logging.getLogger()
handler=logging.FileHandler(LOG_FILENAME)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
handler.setFormatter(formatter)
logger.addHandler(handler)
logger.setLevel(logging.INFO)

class update_doc(threading.Thread):
    def __init__(self,doc_queue,ix):
        threading.Thread.__init__(self)
        self.doc_queue = doc_queue
        self.ix = ix
    def run(self):
        while(True):
            jobs = self.doc_queue.get(1)
            writer = self.ix.writer()
            for j in jobs:
                writer.update_document(id=j[0],orgid=j[1],
                        title=j[2],tags=j[3]
                        ,ishunterjob=j[4])
                logger.info('update doc :'+str(j[0]))
            writer.commit()

class delete_doc(threading.Thread):
    def __init__(self,doc_queue,ix):
        threading.Thread.__init__(self)
        self.doc_queue = doc_queue
        self.ix = ix
    def run(self):
        while(True):
            id = self.doc_queue.get(1)
            self.ix.delete_by_term('id',id)
            logger.info('del doc :'+str(id))

class ADIndex:

    def get_mac_address(self):
        node = uuid.getnode()
        mac = uuid.UUID(int = node).hex[-12:]
        return mac

    def __init__(self,indexdir):
        exists = index.exists_in(indexdir)
        if exists :
            self.ix =index.open_dir(indexdir)
        else:
            schema = Schema(title=TEXT(stored=True),
                    id=NUMERIC(unique=True,stored=True),
                    orgid=NUMERIC(stored=True),
                    ishunterjob=NUMERIC(stored=True),
                    tags=KEYWORD(stored=True)
                    )
            self.ix = create_in(indexdir, schema)
        self.mac_address=self.get_mac_address()
        self.conn = pymongo.Connection(config.MONGO_CONN)
        self.tagsParser = Trie(config.SKILL_FILE)
        self.cache = LRUCache(1024)
        self.add_doc_queue = Queue(1024)
        self.del_doc_queue = Queue(1024)
        self.update_doc_index_thread = update_doc(self.add_doc_queue,self.ix)
        self.update_doc_index_thread.start()
        self.delete_doc_index_thread = delete_doc(self.del_doc_queue,self.ix)
        self.delete_doc_index_thread.start()
     
    def add_doc(self,jobs):
        self.add_doc_queue.put(jobs)
        return {'add doc size':len(jobs)}
    def del_doc(self,id):
        self.del_doc_queue.put(id)
        return {'del doc ':id}
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
        pageurls = self.conn.pongo.pageurls
        url = unicode(url)
        one = pagetags.find_one({"_id": url}, {"tags": 1})
        if one :
            tags = one["tags"]
            tags = tags.replace(',',' OR ')
            return self.find(tags,limit)
        else:
            pageurls.insert({"_id":url})
            logger.info('adv insert :'+url)
            return None
    def jobs2json(self,jobs):
        rep = {}
        rep["server"] = self.mac_address
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

    def cut(self,value):
        value=value.lower().replace('&nbsp','')
        value = value.encode('UTF-8')
        terms = self.tagsParser.parse(value)
        v = {}
        for i in terms:
            v[i[0]]=i[1]
        return v.values()

    def get_cache(self,k):
        if self.cache == None:
            return None
        if k in self.cache:
            m = time.time() - self.cache.mtime(k)
            #当缓存更新时间超过5分钟则删除缓存
            if m > 60 :
                del self.cache[k]
                #logger.info('del cache:'+k+'==>'+m)
                return None
            else:
                return self.cache[k]
        else:
            return None
    def add_cache(self,k,rep):
        if self.cache == None:
            return None
        self.cache[k] = rep
    def del_cache(self):
        self.cache = LRUCache(1024)
        return {'cache size':len(self.cache)}
    def dispatch_hander(self,worker,frames):
        header = frames[2]
        data = frames[3]
        rep = ''
        try:
            #-----------
            mkey = ''
            #走缓存出结果
            if header == 'search':
                m = md5()
                m.update(data)
                mkey = m.hexdigest()
                rep = self.get_cache(mkey)
                if rep != None:
                    rep = json.dumps(rep)
                    msg = [frames[0],frames[1],rep.encode('UTF-8')]
                    worker.send_multipart(msg)
                    logger.info('search get_cache:'+mkey)
                    return 
            #无缓存流程
            jdata = json.loads(data.replace("''","0"),strict=False)
            action = jdata ["action"]
            rep = 'request err :'+data
            if header == 'update' and action == "updateDoc":
                jobs=[]
                for j in jdata['fields']:
                    tags = self.cut(j['jobname']+' '+j['description'])
                    jobid = j['jobid']
                    orgid = j['orgid']
                    jobname = unicode(j['jobname'])
                    tags = ' '.join(tags).decode('UTF-8')
                    ishunterjob=j['ishunterjob']
                    jobs.append((jobid,orgid,jobname,tags,ishunterjob))

                rep = self.add_doc(jobs)
            #remove
            #{"action":"removeDoc","name":"job","keyID":"64983"}
            if header == 'remove' and action == "removeDoc":
                keyid = jdata ["keyID"]
                rep =self.del_doc(int(keyid))
            if header == 'cacheclean':
                rep = self.del_cache()            
            if header == 'search':
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
                #搜索结果添加缓存
                self.add_cache(mkey,rep)
            #-------
        except:
            logger.error("except:"+str(frames))

        rep = json.dumps(rep)
        msg = [frames[0],frames[1],rep.encode('UTF-8')]
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
