import sys
import zmq
from  multiprocessing import Process
import time

NNN=500000
addr = "tcp://127.0.0.1:5559"

def worker():
    context = zmq.Context()
    work_receiver = context.socket(zmq.PULL)
    work_receiver.connect(addr)

    for task_nbr in range(NNN):
        message = work_receiver.recv_pyobj()

    sys.exit(1)

def main():
    Process(target=worker, args=()).start()
    context = zmq.Context()
    ventilator_send = context.socket(zmq.PUSH)
    ventilator_send.bind(addr)
    for num in range(NNN):
        ventilator_send.send_pyobj("MESSAGE")

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    duration = end_time - start_time
    msg_per_sec = NNN / duration

    print("Duration: {}".format(duration))
    print("Messages Per Second: {}".format(msg_per_sec))
#Duration: 2.77
#Messages Per Second: 180 263.06
