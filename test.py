import os
import json
import time
import pdb

from multiprocessing import Process, Queue


class Monitor:

    def __init__(self, conf_file):
        self.conf = json.load(open(conf_file))
        self.state = Queue() # Must pass in manually to Process constructors

    def child(self, state):
        try:
            time.sleep(0.01)
            print("Hello from: ", os.getpid())
            time.sleep(0.01)
            print(b)
        except Exception as e:
            print("error")
            state.put((os.getpid(), e))

    
    def start(self):
        p = Process(target=self.child, args=(self.state,))
        p.start()

m = Monitor(conf_file="conf.json")
m.start()
time.sleep(1)
print(m.state.get())
