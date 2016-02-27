import time
import zmq
from  multiprocessing import Process

NNN=10
addr_vent = "tcp://127.0.0.1:5557"
addr_work_recv = "tcp://127.0.0.1:5558"
addr_work_send = "tcp://127.0.0.1:5559"

# The "ventilator" function generates a list of numbers from 0 to 10000, and 
# sends those numbers down a zeromq "PUSH" connection to be processed by 
# listening workers, in a round robin load balanced fashion.

def ventilator():
    # Initialize a zeromq context
    context = zmq.Context()

    # Set up a channel to send work
    ventilator_send = context.socket(zmq.PUSH)
    ventilator_send.bind(addr_vent)

    # Give everything a second to spin up and connect
    time.sleep(1)

    # Send the numbers between 1 and 1 million as work messages
    for num in range(NNN):
        work_message = { 'num' : num }
        ventilator_send.send_json(work_message)

    time.sleep(1)

# The "worker" functions listen on a zeromq PULL connection for "work" 
# (numbers to be processed) from the ventilator, square those numbers,
# and send the results down another zeromq PUSH connection to the 
# results manager.

def worker(wrk_num):
    # Initialize a zeromq context
    context = zmq.Context()

    # Set up a channel to receive work from the ventilator
    work_receiver = context.socket(zmq.PULL)
    work_receiver.connect(addr_vent)

    # Set up a channel to send result of work to the results reporter
    results_sender = context.socket(zmq.PUSH)
    results_sender.connect(addr_work_recv)

    # Set up a channel to receive control messages over
    control_receiver = context.socket(zmq.SUB)
    control_receiver.connect(addr_work_send)
    control_receiver.setsockopt(zmq.SUBSCRIBE, bytes("",'UTF-8'))

    # Set up a poller to multiplex the work receiver and control receiver channels
    poller = zmq.Poller()
    poller.register(work_receiver, zmq.POLLIN)
    poller.register(control_receiver, zmq.POLLIN)

    # Loop and accept messages from both channels, acting accordingly
    while True:
        socks = dict(poller.poll())

        # If the message came from work_receiver channel, square the number
        # and send the answer to the results reporter
        if socks.get(work_receiver) == zmq.POLLIN:
            work_message = work_receiver.recv_json()
            product = work_message['num'] * work_message['num']
            answer_message = { 'worker' : wrk_num, 'result' : product }
            results_sender.send_json(answer_message)

        # If the message came over the control channel, shut down the worker.
        if socks.get(control_receiver) == zmq.POLLIN:
            control_message = control_receiver.recv()
            if control_message == bytes("FINISHED",'UTF-8'):
                print("Worker {:d} received FINSHED, quitting!".format(wrk_num))
                break

# The "results_manager" function receives each result from multiple workers,
# and prints those results.  When all results have been received, it signals
# the worker processes to shut down.

def result_manager():
    # Initialize a zeromq context
    context = zmq.Context()
    
    # Set up a channel to receive results
    results_receiver = context.socket(zmq.PULL)
    results_receiver.bind(addr_work_recv)

    # Set up a channel to send control commands
    control_sender = context.socket(zmq.PUB)
    control_sender.bind(addr_work_send)

    for task_nbr in range(NNN):
        result_message = results_receiver.recv_json()
        print("Worker {} answered: {}".format(result_message['worker'], result_message['result']))

    # Signal to all workers that we are finsihed
    control_sender.send(bytes("FINISHED",'UTF-8'))
    time.sleep(5)

if __name__ == "__main__":

    # Create a pool of workers to distribute work to
    worker_pool = range(10)
    for wrk_num in range(len(worker_pool)):
        Process(target=worker, args=(wrk_num,)).start()

    # Fire up our result manager...
    result_manager = Process(target=result_manager, args=())
    result_manager.start()

    # Start the ventilator!
    ventilator = Process(target=ventilator, args=())
    ventilator.start()
