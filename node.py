import sys
import socket
import selectors
import types
import logging
from multiprocessing import Process
# import threading
import time
import os



logging.getLogger().setLevel(logging.INFO)
logging.basicConfig(format='[%(message)s]')
format='utf_8'
nodeports=[5050+i for i in range(49)]
MESSAGE_TYPES=[b'node_info',b'request',b'ack',b'task',b'yes',b'no',b'finished',b'stop']
STATES=['init','connecting','idle','processing']


class Node:
    def __init__(self,id,os):
        self.node_id=id
        self.state='init'
        self.info=(nodeports[id],os)
        self.buffer=[]
        self.con_counter=0
        self.ready_counter=0
        self.nready_counter=0
        self.connected_nodes=dict()
        self.range=[4200,4600]  #change at will
        self.results=[]
        self.done_counter=0
        # self.is_requesting=False
        # self.executeList=[]

        """SCENARIO for the demo
        replace all send_IPOS with discover_network
        and also replace all calls to send_REQ with send_requests
        """
        if(id==22):
            p0=runproc(self.connect_to_network,arguments=())
            p1=runproc(self.send_IPOS,arguments=(5083,))
            p1.join()
            p2=runproc(self.send_IPOS,arguments=(5084,))
            p2.join()
            p5=runproc(self.send_IPOS,arguments=(5085,))
            p5.join()
            p7=runproc(self.send_IPOS,arguments=(5086,))
            p7.join()

            time.sleep(4)

            logging.info('SENDING REQUESTS')

            p3=runproc(self.send_REQ,arguments=(5083,'ubuntu v2.6'))
            p3.join()
            p6=runproc(self.send_REQ,arguments=(5084,'ubuntu v2.6'))
            p6.join()
            p4=runproc(self.send_REQ,arguments=(5085,'ubuntu v2.6'))
            p4.join()
            p8=runproc(self.send_REQ,arguments=(5086,'ubuntu v2.6'))
            p8.join()

            # p9=runproc(self.show_results,arguments=())
            # p9.join()

            p0.join()
        else:
            p1=runproc(self.connect_to_network,arguments=())
            # self.send_IPOS(5072)
            p1.join()


    def connect_to_network(self):
            host,port ='127.0.0.1',self.info[0]
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as lsock:
                lsock.bind((host, port))
                lsock.listen()
                print("listening on", (host, port))
                lsock.setblocking(False)
                sels.register(lsock, selectors.EVENT_READ, data=None)
                try:
                    while True:
                        # print("listening on", (host, port))
                        events = sels.select(timeout=None)
                        for key, mask in events:
                            if key.data is None:
                                accept_wrappers(key.fileobj,self)
                            else:
                                service_connections(key, mask,self)

                except KeyboardInterrupt:
                    print("caught keyboard interrupt, exiting")
                finally:
                    sels.close()
    #to test
    """
    def discover_network(self):
        for id in range(50):
            try:
                p=runproc(send_IPOS,arguments=(get_port(id)))
            except:
                pass
    #to test
    
    def send_requests(self):
        for id in self.connected_nodes.keys():
            p=runproc(self.send_REQ,arguments=(id,'ubuntu v2.6'))
            p.join()
    """
    def handle_messages(self):
        allmsgs=''
        for i in self.buffer:
            allmsgs+=i
        rc=allmsgs.split('SEP') #real content of a message
        msg_type,sender=rc[0],int(rc[1])

        if msg_type=='IPOS':
            self.send_ACK(sender)
        if msg_type=='ACK':
            self.connected_nodes[sender]='connected'
            self.con_counter+=1
        if msg_type=='YES':
            self.connected_nodes[sender]='ready'
            self.ready_counter+=1
            if(self.con_counter-self.ready_counter-self.nready_counter==0):
                self.send_job()
        if msg_type=='NO':
            self.connected_nodes[sender]='not ready'
            self.nready_counter+=1
            if(self.con_counter-self.ready_counter-self.nready_counter==0 and self.ready_counter!=0):
                self.send_job()
        if msg_type=='REQ':
            if self.info[1]==rc[2]:
                self.send_YES(sender)
            else:
                self.send_NO(sender)
        if msg_type=='TASK':
            results=[]
            a=int(rc[2].split(',')[0])
            b=int(rc[2].split(',')[1])
            print(a,b,'\n')
            processLL(results,a,b)
            # pll=runproc(processLL,arguments=(a,b))
            # pll.join()
            self.send_FIN(sender,results)
        if msg_type=='FIN':
            self.connected_nodes[sender]='done'
            self.done_counter+=1
            for i in rc[2].split(','):
                self.results.append(i)
            if(self.done_counter==self.ready_counter):
                p9=runproc(self.show_results,arguments=())
                p9.join()

    def send_IPOS(self,destination):
            ipos=make_data_obj(byte_cast(self.info),b'IPOS',('127.0.0.1',destination),self.info[0])
            time.sleep(0.2)
            runproc(sendmessage,(ipos,))
            print('\n')
    def send_ACK(self,destination):
        ack=make_data_obj(byte_cast((self.info[1],)),b'ACK',('127.0.0.1',destination),self.info[0])
        time.sleep(0.2)
        runproc(sendmessage,(ack,))
        print('\n')
    def send_YES(self,destination):
            yes=make_data_obj(byte_cast([]),b'YES',('127.0.0.1',destination),self.info[0])
            time.sleep(0.2)
            runproc(sendmessage,(yes,))
            print('\n')
    def send_NO(self,destination):
            no=make_data_obj(byte_cast([]),b'NO',('127.0.0.1',destination),self.info[0])
            time.sleep(0.2)
            runproc(sendmessage,(no,))
            print('\n')
    def send_TASK(self,destination,range):
            task=make_data_obj(byte_cast((range[0],',',range[1])),b'TASK',('127.0.0.1',destination),self.info[0])
            time.sleep(0.2)
            runproc(sendmessage,(task,))
            print('\n')
    def send_REQ(self,destination,os):
            request=make_data_obj(byte_cast((os,)),b'REQ',('127.0.0.1',destination),self.info[0])
            time.sleep(0.2)
            runproc(sendmessage,(request,))
            print('\n')
    def send_FIN(self,destination,results):
            fin=make_data_obj(byte_cast(results),b'FIN',('127.0.0.1',destination),self.info[0])
            time.sleep(0.2)
            runproc(sendmessage,(fin,))
            print('\n')
    def send_job(self):

        ranges=slice(self.range,self.ready_counter)
        i=0
        for k in [k for k,v in self.connected_nodes.items() if v=='ready']:
            self.send_TASK(k,ranges[i])
            i+=1

    def show_results(self):
        print('\nRESULTS:',end=' ')
        for i in self.results:
            if i:
                print(i,end='\t')
# SERVER vars
sels = selectors.DefaultSelector()


# CLIENT methods
def processLL(r,a,b):
    # task loop
    for i in range(a,b):
        ll=LucasLehmer(i)
        print(i,' ',ll)
        if ll:
            print('\n///////////FOUND PRIME////////////\n')
            r.append(i)
            r.append(',')
def LucasLehmer(p):
    n=2**p-1
    s0=4
    for i in range(1,p-1):
        s1=(s0*s0 -2)%n
        s0=s1
    if(s1==0):
        return True
    else :
        return False
def byte_cast(strings):
    list=[]
    format='utf_8'
    for string in strings:
        list.append(bytes(str(string),format))
    return list
def string_cast(da):
    list=[]
    format='utf_8'
    for d in da:
        list.append(str(d),format)
    return list
def get_id(port):
    return port-5050
def get_port(id):
    return id+5050
def slice(lis,num):
    #devides a list on a number, returns list of couples(start end)
    intrvl=[int(i*((lis[1]-lis[0]+1)/num))+lis[0] for i in range(num+1)]
    return [(intrvl[i-1],intrvl[i]) for i in range(1,num+1)]
def embed_type(messages,type):
    messages.insert(0,b'SEP')
    messages.insert(0,type)
    return messages
def make_data_obj(msgs,type,dadr,sport):
    # make a structure
    data = types.SimpleNamespace(
        msg_total=len(type)+3,
        recv_total=0,
        type=type,
        msgs=list(msgs),
        outb=b"",
        destination=dadr,
        sport=sport,
        source=socket.gethostbyname(socket.gethostname()),
        )
    data.msgs=embed_type(data.msgs,bytes(str(data.sport),'utf_8'))
    data.msg_total+=len(str(data.sport))
    data.msg_total+=sum(len(m) for m in data.msgs)
    data.msgs=embed_type(data.msgs,data.type)
    return data
class message:
    def __init__(self,data):
        self.data=data
        self.selc = selectors.DefaultSelector()
    def send(self):
        print("connecting to ",self.data.destination[1])
        sockc = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sockc.setblocking(False)
        sockc.connect_ex(self.data.destination)
        events = selectors.EVENT_READ | selectors.EVENT_WRITE
        self.selc.register(sockc, events, data=self.data)
    def service_connectionc(self,key, mask):
            sockc = key.fileobj
            data = key.data
            format='utf-8'
            if mask & selectors.EVENT_READ:
                recv_data = sockc.recv(1024)  # Should be ready to read
                if recv_data:
                    logging.info("received")
                    print(recv_data.decode(format))
                    data.recv_total += len(recv_data)
                    if not recv_data or data.recv_total >= data.msg_total:
                        print("closing connection")
                        self.selc.unregister(sockc)
                        sockc.close()
            if mask & selectors.EVENT_WRITE:
                if not data.outb and data.msgs:
                    data.outb = data.msgs.pop(0)
                if data.outb:
                    print('sending ')
                    # data.outb=bytes(data.outb,format)
                    print(data.outb.decode(format))
                    sent = sockc.send(data.outb)  # Should be ready to write
                    data.outb = data.outb[sent:]
    def con_loop(self):
        try:
            while True:
                events = self.selc.select(timeout=1)
                if events:
                    for key, mask in events:
                        self.service_connectionc(key, mask)
                        # logging.info('con if events for')
                    if not self.data.msgs: #saviour, literally
                            break
                # Check for a socket being monitored to continue.
                if not self.selc.get_map():
                    break
                # logging.info('con')
        except KeyboardInterrupt:
            print("caught keyboard interrupt, exiting")
# SERVER methods
def accept_wrappers(socks,nodex):
    nodex.buffer=[]
    conn, addr = socks.accept()  # Should be ready to read
    print("accepted connection from", addr)
    conn.setblocking(False)
    data = types.SimpleNamespace(addr=addr, inb=b"", outb=b"")
    events = selectors.EVENT_READ | selectors.EVENT_WRITE
    sels.register(conn, events, data=data)
def service_connections(key, mask, nodex):
    socks = key.fileobj
    data = key.data
    format='utf-8'
    # rcvd_msgs=[]
    if mask & selectors.EVENT_READ:
        recv_data = socks.recv(1024)  # Should be ready to read
        if recv_data:
            decoded_msg=recv_data.decode(format)
            print("received",decoded_msg)
            nodex.buffer.append(decoded_msg)
            # data.outb += recv_data
        else:
            print("closing connection to", data.addr)
            logging.info('BUFFER')
            for msag in nodex.buffer:
                logging.info(msag)
            nodex.handle_messages()
            sels.unregister(socks)
            socks.close()
    if mask & selectors.EVENT_WRITE:
        pass
        """
        if data.outb:
            decoded_msg=data.outb.decode(format)
            buffer.append(decoded_msg)
            print("echoing", decoded_msg, "to", data.addr)
            sent = socks.send(data.outb)  # Should be ready to write
            data.outb = data.outb[sent:]
            """
def disconnect(): #DOES NOT WORK
    lsock.close()
    sels.close()

if len(sys.argv) != 2 or int(sys.argv[1])<0 or int(sys.argv[1])>49:
    print("usage:", sys.argv[0], "<node_id> 49 or less")
    sys.exit(1)
node_id_entry=int(sys.argv[1])



def runproc(fun,arguments):
    #run a function in a process ,don't forget to join
    p=Process(target=fun,args=arguments)
    p.start()
    return p
def sendmessage(data):
    #creates a message structure and sends it
    ms1=message(data)
    time.sleep(1)
    ms1.send()
    ms1.con_loop()
    ms1.selc.close()


if __name__=='__main__':
    nodea=Node(node_id_entry,'ubuntu v2.6')
    # runproc(target=nodea.send_IPOS())
