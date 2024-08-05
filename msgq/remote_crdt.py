from itertools import count
from dataclasses import dataclass

import multiprocessing as mp
from time import sleep

import zmq

bind_to = 'tcp://*:8993'
#bind_to = 'ipc:///tmp/pzmq.ipc'

class Pub:
    def __init__(self, bind_to):
        self.ctx = zmq.Context()
        self.s = self.ctx.socket(zmq.PUB)
        self.s.bind(bind_to)

    def send(self, a):
        self.s.send_pyobj(a)

    def __del__(self):
        self.s.close(1)
        self.ctx.term()

connect_to = 'tcp://127.0.0.1:8993'
#connect_to = 'ipc:///tmp/pzmq.ipc'

class Sub:
    def __init__(self, connect_to):
        self.ctx = zmq.Context()
        self.s = self.ctx.socket(zmq.SUB)
        self.s.connect(connect_to)
        self.s.setsockopt(zmq.SUBSCRIBE, b'')


    def recv(self):
        ob= self.s.recv_pyobj()
        return ob

    def __del__(self):
        self.s.close(1)
        self.ctx.term()


#pub = Pub(bind_to)



@dataclass(frozen=True)
class Op:
    key: str
    value: str
    sender: str
    sender_timestamp: int



class CRDT:
    def __init__(self, name, pub):
        self.cache = {}
        # through position in ops, the messages are ordered/indexed
        self.ops = set()
        self.counter = 0
        self.name = name
        
        self.neighbors = set()
        self.sub = Sub(connect_to)
        self.pub = pub

    def add_neighbor(self, n):
        self.neighbors.add(n)
        n.neighbors.add(self)

    def senders(self):
        senders = sorted(set([op.sender for op in self.ops]))
        return senders

    def timestamp(self):
        return self.counter

    def lastop(self, sender):
        return max(filter(lambda op: op.sender == sender, self.ops), 
                key=lambda op: op.sender_timestamp)

    def avg_timestamp(self):
        lastops = []
        for s in self.senders():
            lastops.append(self.lastop(s).sender_timestamp)

        if lastops == []:
            return 0
        else:
            avg_timestamp = sum(lastops) / len(lastops)
            return avg_timestamp

    def put(self, key, value):
        c = self.counter
        msg = Op(key, value, self.name, c)
        self.ops.add(msg)

        self.pub.send(['update', [key, value, self.name, c]])
        '''
        notify_neighbors = True
        if notify_neighbors:
            for n in sorted(self.neighbors):
                n.update(key,value, self.name, c)
        '''


    def update(self, key, value, sender, c):
        if sender == self.name:
            return
        msg = Op(key, value, sender, c)
        self.ops.add(msg)

    # very simple idea, ticks have to be provided by a all-known shared clock
    # for example the sun would be a good shared clock, but too slow (day)
    # this code assumes, that ticks are called in lock step for all states
    # through this we can recognize running ahead-clocks and discard those crdts
    # ticks have to be called between any put or upgrade
    def tick(self):
        res = self.sub.recv()
        print(res)
        if res == 'tick':
            self.counter += 1
        elif isinstance(res, list):
            msg = res[1]
            self.update(*mgs)
        else:
            assert False, 'bad res'

    def __lt__(self, b):
        return self.name < b.name


    def flush(self):
        ''' flushes ops to the cache '''
        state = {}

        keys = sorted(set([op.key for op in self.ops]))
        for k in keys:
            for s in self.senders():
                # the value with the highest timestamp
                lastop = self.lastop(s)
                #if lastop.sender == self.name or lastop.sender_timestamp < self.timestamp() + 1:
                if lastop.sender_timestamp < self.avg_timestamp() + 1:
                    state[k] = lastop.value
                # blindly trust our own clock
                elif lastop.sender == self.name:
                    state[k] = lastop.value
                else:
                    print(f'warning, {lastop} is too far ahead, ignoring')

        self.cache = state
        return state


    def get(self, k):
        return self.state[k]


#class RemoveCRDT:
from uuid import uuid4



def crd(pub):
    name = str(uuid4())
    a = CRDT(name, pub)

    for v in range(10):
        a.tick()
        a.put('key', str(v))

        print(f'{name}, {a.flush()}')
    print(f'{name}, {a.flush()}')


pub = None
def crd2(pub):
    pub = Pub(bind_to)

    def tick():
        pub.send('tick')

    for v in range(100):
        tick()
        sleep(1)
        print('ticka')

    #print(f'{name}, {b.flush()}')
    #print(f'{name}, {b.flush()}')

#p = mp.Process(target=pub)

p2 = mp.Process(target=crd, args=[pub])
p3 = mp.Process(target=crd2, args=[pub])
p2.start()
sleep(1)
p3.start()
#p.start()
#p.join()
p2.join()
p3.join()
