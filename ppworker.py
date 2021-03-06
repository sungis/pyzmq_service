#!/usr/bin/env python
# -*- coding: utf-8 -*-
'''
#=============================================================================
#     FileName: ppworker.py
#         Desc: 广告检索
#       Author: Sungis
#        Email: mr.sungis@gmail.com
#     HomePage: http://sungis.github.com
#      Version: 0.0.1
#   LastChange: 2013-06-21 19:51:37
#      History:
#=============================================================================
'''
#
##  Paranoid Pirate worker
#
#   Author: Daniel Lundin <dln(at)eintr(dot)org>
#

from random import randint
import time
import zmq
from ad_service import ADIndex,update_task
import sys
import config
from multiprocessing import Process, Queue
from whoosh import index
from whoosh.fields import *

logger = config.getLogger('ppworker',"./data/ppworker.log")

HEARTBEAT_LIVENESS = config.HEARTBEAT_LIVENESS     # 3..5 is reasonable
HEARTBEAT_INTERVAL = config.HEARTBEAT_INTERVAL   # Seconds

INTERVAL_INIT = config.INTERVAL_INIT
INTERVAL_MAX = config.INTERVAL_MAX

#  Paranoid Pirate Protocol constants
PPP_READY = config.PPP_READY      # Signals worker is ready
PPP_HEARTBEAT = config.PPP_HEARTBEAT  # Signals worker heartbeat

INDEX_PATH="indexdir"
HOST = 'localhost'
WORKER_HOST="tcp://localhost:5556"
SUBSCRIBER_HOST="tcp://localhost:5557"


mac_address = config.get_mac_address()

def worker_socket(context, poller,pid):
    """Helper function that returns a new configured socket
       connected to the Paranoid Pirate queue"""
    worker = context.socket(zmq.DEALER) # DEALER
    identity = "work-%s-%d:%04X-%04X" % (mac_address,pid,randint(0, 0x10000), randint(0, 0x10000))
    worker.setsockopt(zmq.IDENTITY, identity)
    poller.register(worker, zmq.POLLIN)
    worker.connect(WORKER_HOST)
    worker.send(PPP_READY)
    return worker

def dispatch_hander(worker,frames):
    logger.info("I: Normal reply")
    worker.send_multipart(frames)
    liveness = HEARTBEAT_LIVENESS
    time.sleep(1)  # Do some heavy work


def subscriber_socket(context,poller,pid):
    subscriber = context.socket(zmq.SUB)  # SUB
    identity = "sub-%s-%d:%04X-%04X" % (mac_address,pid,randint(0, 0x10000), randint(0, 0x10000))
    subscriber.setsockopt(zmq.IDENTITY, identity)
    subscriber.setsockopt(zmq.SUBSCRIBE, '')
    poller.register(subscriber, zmq.POLLIN)
    subscriber.connect(SUBSCRIBER_HOST)
    return subscriber



def work_hander(ix,task_queue,pid,has_sub = True):

    ad_idx = ADIndex (ix,task_queue,pid)

    context = zmq.Context(1)
    poller = zmq.Poller()

    liveness = HEARTBEAT_LIVENESS
    interval = INTERVAL_INIT

    heartbeat_at = time.time() + HEARTBEAT_INTERVAL

    subscriber = None 
    
    if has_sub:
        subscriber = subscriber_socket(context,poller,pid)

    worker = worker_socket(context, poller,pid)
    #cycles = 0
    while True:
        socks = dict(poller.poll(HEARTBEAT_INTERVAL * 1000))
        if has_sub and socks.get(subscriber) == zmq.POLLIN:
            frames = subscriber.recv_multipart()
            if not frames:
                break
            if len(frames) == 4:
                ad_idx.dispatch_hander(worker,frames)
                liveness = HEARTBEAT_LIVENESS

        # Handle worker activity on backend
        if socks.get(worker) == zmq.POLLIN:
            #  Get message
            #  - 3-part envelope + content -> request
            #  - 1-part HEARTBEAT -> heartbeat
            frames = worker.recv_multipart()
            if not frames:
                break # Interrupted

            if len(frames) > 4:
                ad_idx.dispatch_hander(worker,frames)
                liveness = HEARTBEAT_LIVENESS
                # Simulate various problems, after a few cycles
                #cycles += 1
                #if cycles > 3 and randint(0, 5) == 0:
                #    print "I: Simulating a crash"
                #    break
                #if cycles > 3 and randint(0, 5) == 0:
                #    print "I: Simulating CPU overload"
                #    time.sleep(3)
                #print "I: Normal reply"
                #worker.send_multipart(frames)
                #liveness = HEARTBEAT_LIVENESS
                #time.sleep(1)  # Do some heavy work
            elif len(frames) == 1 and frames[0] == PPP_HEARTBEAT:
#                logger.info("I: Queue heartbeat")
                liveness = HEARTBEAT_LIVENESS
            else:
                logger.info("E: Invalid message: %d %s" % (len(frames),frames))
            interval = INTERVAL_INIT
        else:
            liveness -= 1
            if liveness == 0:
                logger.info("W: Heartbeat failure, can't reach queue")
                logger.info("W: Reconnecting in %0.2fs..." % interval)
                time.sleep(interval)

                if interval < INTERVAL_MAX:
                    interval *= 2
                poller.unregister(worker)
                worker.setsockopt(zmq.LINGER, 0)
                worker.close()
                worker = worker_socket(context, poller,pid)
                
                if has_sub :
                    poller.unregister(subscriber)
                    subscriber.close()
                    subscriber = subscriber_socket(context,poller,pid)

                liveness = HEARTBEAT_LIVENESS
        if time.time() > heartbeat_at:
            heartbeat_at = time.time() + HEARTBEAT_INTERVAL
#            logger.info("I: Worker heartbeat")
            worker.send(PPP_HEARTBEAT)
if __name__ == '__main__':
    '''

    python ppworker.py indexdir 10.0.1.77

    '''
    if len(sys.argv)==3:
        INDEX_PATH = sys.argv[1]
        HOST = sys.argv[2]
        WORKER_HOST="tcp://"+HOST+":5556"
        SUBSCRIBER_HOST = "tcp://"+HOST+":5557"
        

    logger.info('INDEX_PATH:%s HOST:%s' %(INDEX_PATH , HOST))
    
    ix = None
    exists = index.exists_in(INDEX_PATH)
    if exists :
        ix =index.open_dir(INDEX_PATH)
    else:
        schema = Schema(title=TEXT(stored=True),
                id=NUMERIC(unique=True,stored=True),
                orgid=NUMERIC(stored=True),
                ishunterjob=NUMERIC(stored=True),
                tags=KEYWORD(stored=True)
                )
        ix = create_in(INDEX_PATH, schema)
    
    task_queue = Queue(2048)
    ptask = update_task(task_queue,ix)
    ptask.start()
    for i in range(5):        
        p = Process(target = work_hander, args=(ix,task_queue,i,i==0))
        p.start()


