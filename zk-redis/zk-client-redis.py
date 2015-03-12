#-*-coding:utf-8-*-
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

class RedisRegister(threading.Thread):
    def __init__(self, host, port):
        threading.Thread.__init__(self)
        self.host = host
        self.port = port
        self.lastAlive = time.time()
        self.client = None
        self.expired = threading.Event()
        self.logger=logging.getLogger('redis')
        self.enable = True

    def run(self):
        while(self.enable):
            try:
                if self.checkalive():
                    if(self.client == None):
                        self.connect()
                        
                    elif (self.expired.is_set()):
                        self.reconnect()
                        self.expired.clear()
                    
                    self.lastAlive = time.time()
                    self.log('get nodes:',self.client.get_children(settings.zkRootPath))
                    ll = self.client.get_children(settings.zkRootPath)
                    #self.log('get nodes dddd:',self.client.__dict__)
                    #for it in ll:
                    #    self.log('get nodes ccc:',self.client.get(settings.zkRootPath + '/%s'% it))
                else:
                    if(time.time() - self.lastAlive > settings.checkTimeoutSec):
                        self.close()
            except Exception, ex:
                self.log(ex)
            finally:
                time.sleep(settings.checkIntervalSec)
    def stop(self):
        self.close()
        self.enable = False
                    
    def checkalive(self):
        ret = False
        try:
            cmd = settings.redisCli + ' -h %s -p %d PING' % (self.host, self.port)
            ret = os.popen(cmd).readline().find("PONG") != -1
        except Exception, e:
            self.log('check alive failed', e)
            
        self.logger.info('[%s:%d] check alive' % (self.host, self.port) + str(ret))
        return ret
           
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
        self.create_node()
 
    def create_node(self):
        self.log('ensure path', settings.zkRootPath)     
        self.client.ensure_path(settings.zkRootPath)
        self.log('begin create node');
        data=self.create_nodeValue()
        self.log('RouteValue:',data)
        self.client.create(settings.zkRootPath + '/' + '%s:%d_%s;' % (self.host, self.port, settings.Weight), value=data, ephemeral=True, sequence=True)
        self.log('create node success')
        
    def create_nodeValue(self):
        node = {}
        node['RoleName'] = '%s:%d' % (self.host, self.port)
        node['Policy'] = settings.Policy
        node['SiteName'] = settings.SiteName
        node['host'] = '%s:%d:%s'%(self.host, self.port, settings.Weight)
        node['RouteId'] = 0
        node['NodeOrder']=0
        node['Enabled'] = 1
        node['RouteValue'] = settings.RouteValue % (self.host, self.port)
        
        return json.dumps(node)
                  
    def close(self):
        client = self.client
        self.client = None
        
        if(client != None):
            self.log('begin close node')
            client.stop()
            client.close()
            self.log('close node success')
    
    def log(self, *msg):
            log= '[%s:%d]' % (self.host, self.port), ' '.join([str(d) for d in msg])
            self.logger.warn(log)

def autoreload():   
    mod_names = ['settings']   
    for mod_name in mod_names:   
        try :   
            module = sys.modules[mod_name]   
        except :   
            continue   
        filename = module.__file__
        if filename.endswith(".pyc"):
            filename = filename.replace(".pyc", ".py")
        mtime =  os.path.getmtime(filename)   
        if not hasattr(module, 'loadtime'):
            setattr(module, 'loadtime', 0)
        try:   
            if mtime > module.loadtime:   
                reload(module)   
        except:   
            pass   
        module.loadtime = mtime           


def exists_instance():
    count=(int)(os.popen('ps -ef | grep redis-manager.py| grep -v grep| wc -l').readline())
    return count>1 # current + exists (2)
                     
if __name__ == '__main__':
    curpath=os.path.split( os.path.realpath( sys.argv[0] ) )[0]
    logging.config.fileConfig(curpath+'/logging.conf')
    
    options,args = getopt.getopt(sys.argv[1:],"ds:",['daemon','singleton'])
    
    isMulti=False
    for name,value in options:
        if name in('-s','--singleton') and value=='False':
            isMulti=True
        
    if(not isMulti and exists_instance()):
        raise Exception("redis-manager instance already exits!!,or use -s False to allow multi-instance!")
    
    for name,value in options:
        if name in ('-d','--daemon'):
            daemonize()

    threads = {}
    module = sys.modules['settings'] 
    filename = module.__file__
    #print 'module, filename:::', module, filename, settings.__dict__		 
    def check_port():
        for node in settings.redis_nodes:
            if not threads.has_key(node):
                host, port = node.split(':')[0].strip('\0'), int(node.split(':')[1].strip('\0'))
                t = RedisRegister(host, port)
                t.setDaemon(True)
                t.start()
                threads[node] = t

    def check_thread():
        for node, t in threads.items():
            if node not in settings.redis_nodes:
                t.stop()
                threads.pop(node)
        
    while(True):
        autoreload()
        check_port()
        check_thread()
        time.sleep(1)
