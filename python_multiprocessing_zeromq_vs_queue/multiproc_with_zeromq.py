#!/usr/bin/python3

import sys
import zmq
from  multiprocessing import Process
import time

NNN=5000000
addr = "tcp://127.0.0.1:5559"

def worker():
    context = zmq.Context()
    work_receiver = context.socket(zmq.PULL)
    work_receiver.connect(addr)

    for task_nbr in range(NNN):
        message = work_receiver.recv()

    sys.exit(1)

def main():
    Process(target=worker, args=()).start()
    context = zmq.Context()
    ventilator_send = context.socket(zmq.PUSH)
    ventilator_send.bind(addr)
    for num in range(NNN):
        ventilator_send.send(bytes("MESSAGE",'UTF-8'))

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    duration = end_time - start_time
    msg_per_sec = NNN / duration

    print("Messages: {:10d}".format(NNN))
    print("Duration: {:10.1f} sec".format(duration))
    print("Rate:     {:10.1f} msg/sec".format(msg_per_sec))

# Duration: 12.12
# Messages Per Second: 412 391.77
