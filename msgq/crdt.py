from itertools import count
from dataclasses import dataclass


@dataclass(frozen=True)
class Op:
    key: str
    value: str
    sender: str
    sender_timestamp: int


class CRDT:
    def __init__(self, name):
        self.cache = {}
        # through position in ops, the messages are ordered/indexed
        self.ops = set()
        self.counter = 0
        self.name = name
        
        self.neighbors = set()

    def add_neighbor(self, n):
        self.neighbors.add(n)

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

        notify_neighbors = True
        if notify_neighbors:
            for n in sorted(self.neighbors):
                n.update(key,value, self.name, c)


    def update(self, key, value, sender, c):
        msg = Op(key, value, sender, c)
        self.ops.add(msg)

    # very simple idea, ticks have to be provided by a all-known shared clock
    # for example the sun would be a good shared clock, but too slow (day)
    # this code assumes, that ticks are called in lock step for all states
    # through this we can recognize running ahead-clocks and discard those crdts
    # ticks have to be called between any put or upgrade
    def tick(self):
        self.counter += 1

    def flush(self):
        for op in self.ops:
            self.state

    def _state(self):
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


a = CRDT('a')
b = CRDT('b')
a.add_neighbor(b)
b.add_neighbor(a)

def tick():
    a.tick()
    b.tick()

a.put('hello', 'a')
tick()
a.put('hello', 'b')
tick()
a.put('hello', 'd')
tick()
b.put('hello', 'd2')

#print(a.ops)

print(a._state())
print(b._state())
