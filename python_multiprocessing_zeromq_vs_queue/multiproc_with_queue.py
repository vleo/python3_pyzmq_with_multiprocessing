#!/usr/bin/python3

import sys
import time
from  multiprocessing import Process, Queue

NNN=1000000

def worker(q):
    for task_nbr in range(NNN):
        message = q.get()

    sys.exit(1)

def main():
    send_q = Queue()
    Process(target=worker, args=(send_q,)).start()
    for num in range(NNN):
        send_q.put("MESSAGE")

if __name__ == "__main__":
    start_time = time.time()
    main()
    end_time = time.time()
    duration = end_time - start_time
    msg_per_sec = NNN / duration

    print("Messages: {:10d}".format(NNN))
    print("Duration: {:10.1f} sec".format(duration))
    print("Rate:     {:10.1f} msg/sec".format(msg_per_sec))

# this yields 97 927 messages/sec on
# 8 core
# vendor_id	: GenuineIntel
# cpu family	: 6
# model		: 23
# model name	: Intel(R) Xeon(R) CPU           E5430  @ 2.66GHz
# stepping	: 6
# cpu MHz		: 1998.000
# cache size	: 6144 KB

