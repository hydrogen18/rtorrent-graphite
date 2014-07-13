import eventlet
eventlet.monkey_patch()

import socket
import xmlrpclib
import sys
import time
import json
import itertools


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
            self.write_metric('transfer.download-rate',dl)
            
            ul = rtorrent.get_up_rate('')
            self.write_metric('transfer.upload-rate',ul)
            
            memory = rtorrent.get_memory_usage('')
            self.write_metric('memory.usage',memory)
            
            max_memory = rtorrent.get_max_memory_usage('')
            self.write_metric('memory.max',max_memory)
            
            download_list = rtorrent.download_list('')
            
            mc = xmlrpclib.MultiCall(rtorrent)
            for dl in download_list:
                mc.__getattr__('d.get_complete')(dl)
                
            for dl in download_list:        
                mc.__getattr__('d.get_left_bytes')(dl)
                
            for dl in download_list:
                mc.__getattr__('d.get_bytes_done')(dl)
                
            result = mc()
            #Wrap so that subsequent use of itertools
            #doesn't start over from the beginning
            result = (r for r in result)

            complete = 0
            incomplete = 0
            
            bytes_remaining = 0
            bytes_downloaded = 0
            
            dummy = range(len(download_list))
            
            for _, isComplete in itertools.izip(dummy,result):
                complete += isComplete
            
            for _, bytes_left in itertools.izip(dummy,result):
                bytes_remaining += bytes_left
                
            for _, bytes_done in itertools.izip(dummy,result):
                bytes_downloaded += bytes_done
                
            incomplete = len(download_list) - complete
            
            self.write_metric('torrents.complete',complete)
            self.write_metric('torrents.incomplete',incomplete)
            self.write_metric('torrents.bytes-done',bytes_downloaded)
            self.write_metric('torrents.bytes-remaining',bytes_remaining)
                    
                
            
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
                    conn.shutdown(socket.SHUT_RDWR)
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
        
