#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
#=============================================================================
#     FileName: ppqueue.py
#         Desc: 广告服务路由
#       Author: Sungis
#        Email: mr.sungis@gmail.com
#     HomePage: http://sungis.github.com
#      Version: 0.0.1
#   LastChange: 2013-06-21 19:50:50
#      History:
#=============================================================================
'''
#
##  Paranoid Pirate queue
#
#   Author: Daniel Lundin <dln(at)eintr(dot)org>
#

from collections import OrderedDict
import time
import sys
import zmq
from hashlib import md5
from cache.lrucache import LRUCache
import config


logger = config.getLogger('ppqueue',"./data/ppqueue.log")

HEARTBEAT_LIVENESS = config.HEARTBEAT_LIVENESS     # 3..5 is reasonable
HEARTBEAT_INTERVAL = config.HEARTBEAT_INTERVAL   # Seconds

#  Paranoid Pirate Protocol constants
PPP_READY = config.PPP_READY      # Signals worker is ready
PPP_HEARTBEAT = config.PPP_HEARTBEAT  # Signals worker heartbeat


class Worker(object):
    def __init__(self, address):
        self.address = address
        self.expiry = time.time() + HEARTBEAT_INTERVAL * HEARTBEAT_LIVENESS

class WorkerQueue(object):
    def __init__(self):
        self.queue = OrderedDict()

    def ready(self, worker):
        self.queue.pop(worker.address, None)
        self.queue[worker.address] = worker

    def purge(self):
        """Look for & kill expired workers."""
        t = time.time()
        expired = []
        for address,worker in self.queue.iteritems():
            if t < worker.expiry:  # Worker is alive
                break
            expired.append(address)
        for address in expired:
            logger.info("W: Idle worker expired: %s" % address)
            self.queue.pop(address, None)

    def next(self):
        address, worker = self.queue.popitem(False)
        return address

class SearchCache(object):
    def __init__(self):
        self.size = 10240
        self.timeout = 6    #600
        self.changedTime = time.time()
        self.cache = LRUCache(self.size)
    def data_changed(self):
        self.changedTime = time.time()
    def get_cache(self,k):
        if self.cache == None:
            return None
        if k in self.cache:
            m = self.changedTime - self.cache.mtime(k)
            #当缓存更新时间 - 更新时间 超过10分钟 则删除缓存
            if m > self.timeout:
                del self.cache[k]
                #logger.info('del cache:'+k+'==>'+m)
                return None
            else:
                logger.info("search get_cache:%s" %mkey)
                return self.cache[k]
        else:
            return None
    def add_cache(self,k,rep):
        if self.cache == None:
            return None
        self.cache[k] = rep
        logger.info("search add_cache:%s" %mkey)
    def del_cache(self):
        self.cache = LRUCache(self.size)
        logger.info('cache size :%d' %len(self.cache))

if __name__ == '__main__':
    context = zmq.Context(1)

    frontend = context.socket(zmq.ROUTER) # ROUTER
    backend = context.socket(zmq.ROUTER)  # ROUTER
    syscend = context.socket(zmq.PUB)     # PUB
    frontend.bind("tcp://*:5555") # For clients
    backend.bind("tcp://*:5556")  # For workers
    syscend.bind("tcp://*:5557")  # For workers

    poll_workers = zmq.Poller()
    poll_workers.register(backend, zmq.POLLIN)

    poll_both = zmq.Poller()
    poll_both.register(frontend, zmq.POLLIN)
    poll_both.register(backend, zmq.POLLIN)

    scache = SearchCache()

    workers = WorkerQueue()

    heartbeat_at = time.time() + HEARTBEAT_INTERVAL

    while True:
        if len(workers.queue) > 0:
            poller = poll_both
        else:
            poller = poll_workers
        socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))

        # Handle worker activity on backend
        if socks.get(backend) == zmq.POLLIN:
            # Use worker address for LRU routing
            frames = backend.recv_multipart()
            if not frames:
                break

            address = frames[0]
            workers.ready(Worker(address))

            # Validate control message, or return reply to client
            msg = frames[1:]
            if len(msg) == 1:
                if msg[0] == PPP_READY:
                    logger.info("address:%s msg:PPP_READY" %(address))
                elif msg[0] == PPP_HEARTBEAT:
                    #logger.info("address:%s msg:PPP_HEARTBEAT" %(address))
                    pass
                else:
                    logger.info("E: Invalid message from worker: %s" % msg)
            else:#转发后台返回的结果 并缓存
                if len(msg) == 4:
                #搜索结果数据包长度为4,TODO 这个特征有局限,容后添加标志位
                    mkey = msg[3]
                    scache.add_cache(mkey,msg[2])
                    msg.pop(3)
                frontend.send_multipart(msg)

            # Send heartbeats to idle workers if it's time
            if time.time() >= heartbeat_at:
                for worker in workers.queue:
                    msg = [worker, PPP_HEARTBEAT]
                    backend.send_multipart(msg)
                heartbeat_at = time.time() + HEARTBEAT_INTERVAL
        if socks.get(frontend) == zmq.POLLIN:
            frames = frontend.recv_multipart()
            if not frames:
                break
            if len(frames) == 4:
                if (frames[2]=='update' or frames[2]=='remove'):
                    syscend.send_multipart(frames)
                    scache.data_changed()
                elif frames[2] == 'cacheclean':
                    scache.del_cache()
                    rep = '{"message":"del cache ok"}'
                    msg = [frames[0],frames[1],rep]
                    frontend.send_multipart(msg)
                elif frames[2]=='search':
            #当时搜索任务时,生成md5,查看是否有缓存
            #是:构造数据包直接返回结果
            #否:数据包中添加md5 发送到后端计算,当返回结果时添加到缓存中
                    data = frames[3]
                    m = md5()
                    m.update(data)
                    mkey = m.hexdigest()
                    rep = scache.get_cache(mkey)
                    if rep != None:
                        msg = [frames[0],frames[1],rep]
                        frontend.send_multipart(msg)
                    else:
                        frames.insert(0, workers.next())
                        frames.append(mkey)
                        backend.send_multipart(frames)
                else:
                    frames.insert(0, workers.next())
                    backend.send_multipart(frames)
            else:
                frames.insert(0, workers.next())
                backend.send_multipart(frames)


        workers.purge()
