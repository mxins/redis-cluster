from kazoo.client import KazooClient
from kazoo.client import KazooState
import time
import threading
import settings
import os
import datetime
import threading
import json
import logging
import logging.config
import sys
import getopt
import tornado.ioloop
import tornado.web
import tornado.autoreload
from hash_ring import HashRing
import random

class RedisNode(threading.Thread):
    nodes = []
    weights = {}
    def __init__(self):
        threading.Thread.__init__(self)
        self.client = None
        self.expired = threading.Event()
        self.logger=logging.getLogger('redis')

    def run(self):
        while True:
            try:
                if(self.client == None):
                    self.connect()
                    
                elif (self.expired.is_set()):
                    self.reconnect()
                    self.expired.clear()
                nodes = self.client.get_children(settings.zkRootPath)
                _nodes, _weights = [], {}
                for it in nodes:
                    _host_port_weight = it.split(';')[0]
                    _host_port, _weight = _host_port_weight.split('_')
                    _nodes.append(_host_port)
                    _weights[_host_port] = int(_weight)
                    RedisNode.nodes = _nodes
                    RedisNode.weights = _weights
                self.log('get nodes:',self.client.get_children(settings.zkRootPath))                
            except Exception, ex:
                self.log(ex)
            finally:
                time.sleep(settings.checkIntervalSec)
           
    def reconnect(self):
        self.log('begin reconnect')
        self.close()
        self.connect()
        self.log('reconnect success')
           
    def connect(self):
        # Once connected, the client will attempt to stay connected regardless of 
        # intermittent connection loss or Zookeeper session expiration.
        #***we need not create new ZookeeperClient When Session Expired like java client
        # but here we recreate new KazooClient when Session Expired
        def conn_listener(state):
            try:
                self.log('connected changed:', state)
                if state == KazooState.LOST:  # Session Expired
                    self.expired.set()
                    self.log('session Expired')
                elif state == KazooState.SUSPENDED:  # disconnected from Zookeeper
                    pass
                else:  # KazooState.CONNECTED ,connected/reconnected to Zookeeper
                    pass
            except Exception, e:
                self.log(e)
        
        self.client = KazooClient(hosts=settings.zkServers, timeout=settings.zkTimeoutSec)
        self.log('begin connect to', settings.zkServers, 'timeout:', settings.zkTimeoutSec, "s")
        self.client.add_listener(conn_listener)
        self.client.start()
        self.log('connect to', settings.zkServers, 'success')
                  
    def close(self):
        client = self.client
        self.client = None
        
        if(client != None):
            self.log('begin close node')
            client.stop()
            client.close()
            self.log('close node success')
    
    def log(self, *msg):
        if settings.debug:
            log= '[11]', ' '.join([str(d) for d in msg])
            self.logger.warn(log)

class MainHandler(tornado.web.RequestHandler):
    def get(self):
        self.write("Nodes: %r, Weights: %r\n"% (RedisNode.nodes, RedisNode.weights))

class GetKeyNode(tornado.web.RequestHandler):
    def get(self, key):
        _hash_ring = HashRing(nodes=RedisNode.nodes, weights = RedisNode.weights)
        node = _hash_ring.get_node(key)
        self.write("key :%s, node: %r\n"% (key, node))


application = tornado.web.Application([
    (r"/", MainHandler),
    (r"^/get_node/([A-Za-z0-9]{1,32}$)", GetKeyNode),
])

                     
if __name__ == '__main__':
    curpath=os.path.split( os.path.realpath( sys.argv[0] ) )[0]
    logging.config.fileConfig(curpath+'/logging.conf')
    
    t = RedisNode()
    t.setDaemon(True)
    t.start()

    application.listen(settings.port)
    instance = tornado.ioloop.IOLoop.instance()
    #tornado.autoreload.start(instance)
    instance.start()
