import eventlet
eventlet.monkey_patch()

import socket
import xmlrpclib
import sys
import time
import json


class Monitor(object):
    def __init__(self,name,url,interval,writer):
        self.url = url
        self.name = name
        self.interval = interval
        self.writer = writer
        
    def __call__(self):
        rtorrent = xmlrpclib.ServerProxy(self.url)
        while True:
            dl = rtorrent.get_down_rate('')
            self.write_metric('download-rate',dl)
            time.sleep(self.interval)
            
    def write_metric(self,metric_name,metric_value):
        self.writer.write_metric("%s.%s" % ( self.name,metric_name),metric_value)
            
        
class Measurement(object):
    def __init__(self,metric_name,metric_value,time):
        self.metric_name = metric_name
        self.metric_value = metric_value
        self.time = time
        
        
class GraphiteWriter(object):
    def __init__(self,host,port,prefix):
        self.host = host
        self.port = port
        self.prefix = prefix
        self.incoming = eventlet.queue.LightQueue(128)
        
    def write_metric(self,metric_name,metric_value):
        m = Measurement(metric_name,metric_value,time.time())
        self.incoming.put(m)
        
    def __call__(self):
        while True:
            conn = socket.create_connection((self.host,self.port),30)
            healthy = True
            while healthy:
                m = self.incoming.get()
                line = '%s.%s %f %d\n' %(self.prefix,m.metric_name,m.metric_value,m.time)
                sys.stdout.write(line)
                try:
                    conn.send(line)
                except socket.error as e:
                    sys.stderr.write("Communications error: %s\n" % e)
                    healthy = False
                    conn.shutdown()
                    conn.close()
                time.sleep(1)
    

if __name__ == "__main__":
    config = {}
    with open(sys.argv[1],'r') as fin:
        config = json.load(fin)
        
    graphite_config = config['graphite']
    writer = GraphiteWriter(**graphite_config)
    sources = config['sources']
    
    for source in sources:
        source['writer'] = writer
        source['interval'] = config['interval']
        mon = Monitor(**source)
        eventlet.spawn(mon)
        
    writer()
        
