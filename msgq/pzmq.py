# pyzeromq pubsub example
import zmq


def sync(bind_to: str) -> None:
    # use bind socket + 1
    sync_with = ':'.join(
        bind_to.split(':')[:-1] + [str(int(bind_to.split(':')[-1]) + 1)]
    )
    ctx = zmq.Context.instance()
    s = ctx.socket(zmq.REP)
    s.bind(sync_with)
    print("Waiting for subscriber to connect...")
    s.recv()
    print("   Done.")
    s.send(b'GO')
    print("And sent.")


def sync2(connect_to: str) -> None:
    # use connect socket + 1
    sync_with = ':'.join(
        connect_to.split(':')[:-1] + [str(int(connect_to.split(':')[-1]) + 1)]
    )
    ctx = zmq.Context.instance()
    s = ctx.socket(zmq.REQ)
    s.connect(sync_with)
    s.send(b'READY')
    print('Waiting for publisher...')
    s.recv()
    print('Publisher done.')


import multiprocessing as mp
from time import sleep

bind_to = 'tcp://*:8993'
#bind_to = 'ipc:///tmp/pzmq.ipc'

def pub():
    a = ['hello']
    ctx = zmq.Context()
    s = ctx.socket(zmq.PUB)
    s.bind(bind_to)

    sync(bind_to)
    #sleep(2)
    #while 1:
    for i in range(5):
        s.send_pyobj(a)
        sleep(1)
    s.close(1)
    ctx.term()


connect_to = 'tcp://127.0.0.1:8993'
#connect_to = 'ipc:///tmp/pzmq.ipc'
def sub():
    ctx = zmq.Context()
    s = ctx.socket(zmq.SUB)
    s.connect(connect_to)
    s.setsockopt(zmq.SUBSCRIBE, b'')

    sync2(connect_to)

    for i in range(5):
    #while 1:
        print("Receiving arrays...")
        a = s.recv_pyobj()
        sleep(1)
        print(f"   Done. {a}")
    s.close(1)
    ctx.term()


p = mp.Process(target=pub)
p2 = mp.Process(target=sub)
p2.start()
p.start()
p.join()
p2.join()
