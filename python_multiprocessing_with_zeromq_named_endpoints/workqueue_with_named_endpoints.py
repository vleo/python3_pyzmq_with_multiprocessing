import time
import zmq
import json
from  multiprocessing import Process

NNN=3
addr_vent = "tcp://127.0.0.1:5557"
addr_work_recv = "tcp://127.0.0.1:5558"
addr_work_send = "tcp://127.0.0.1:5559"

def ventilator(work_addresses):
    context = zmq.Context()

    ventilator_send = context.socket(zmq.PUSH)
    ventilator_send.bind(addr_vent)

    time.sleep(1)

    for num in range(NNN):
        work_message = json.dumps({ 'num' : num })

        # for each worker adddress in our array send a message to that worker
        for work_address in work_addresses:
            
            # sending a multi part message - first part is address of worker,
            # second part is the work message.
            #print("sending to {} value {}".format(work_address, work_message) )
            ventilator_send.send_multipart([bytes(work_address,'UTF-8'), bytes(work_message,'UTF-8')])

    time.sleep(1)

def worker(wrk_num, work_address):
    context = zmq.Context()

    work_receiver = context.socket(zmq.PULL)
    work_receiver.setsockopt(zmq.IDENTITY, bytes(work_address,'UTF-8'))
    work_receiver.connect(addr_vent)

    results_sender = context.socket(zmq.PUSH)
    results_sender.connect(addr_work_recv)

    control_receiver = context.socket(zmq.SUB)
    control_receiver.connect(addr_work_send)
    control_receiver.setsockopt(zmq.SUBSCRIBE, bytes("",'UTF-8'))

    poller = zmq.Poller()
    poller.register(work_receiver, zmq.POLLIN)
    poller.register(control_receiver, zmq.POLLIN)

    while True:
        socks = dict(poller.poll())

        if socks.get(work_receiver) == zmq.POLLIN:

            # get our multi part message, index 0 is address, index 1 is message
            multi_message = work_receiver.recv_multipart()
            work_message = json.loads(multi_message[1].decode('UTF-8'))
            #print("{} received {}".format(work_address,work_message))

            # if we're a "square" worker, square the number
            if work_address == "SQUARE":
                answer = work_message['num'] * work_message['num']

            # if we're a "cube" worker, cube the number
            if work_address == "CUBE":
                answer = work_message['num'] * work_message['num'] * work_message['num']

            answer_message = { 'worker' : wrk_num, 'worker_type' : work_address, 'question' : work_message['num'], 'result' : answer }
            #print("answer_message: {}".format(answer_message))
            results_sender.send_json(answer_message)

        if socks.get(control_receiver) == zmq.POLLIN:
            control_message = control_receiver.recv()
            #print(" {} receieve ctrl {}".format(work_address,control_message))
            if control_message == bytes("FINISHED",'UTF-8'):
                print("Worker %i received FINSHED, quitting!" % wrk_num)
                break

def result_manager():
    # Initialize a zeromq context
    context = zmq.Context()
    
    # Set up a channel to receive results
    results_receiver = context.socket(zmq.PULL)
    results_receiver.bind(addr_work_recv)

    # Set up a channel to send control commands
    control_sender = context.socket(zmq.PUB)
    control_sender.bind(addr_work_send)

    for task_nbr in range(2*NNN):
        result_message = results_receiver.recv_json()
        print("Worker %i of type %s given %i answered: %i" % (result_message['worker'], result_message['worker_type'], result_message['question'], result_message['result']))

    # Signal to all workers that we are finsihed
    control_sender.send(bytes("FINISHED",'UTF-8'))
    time.sleep(5)

if __name__ == "__main__":
    # Create a "square" worker
    square_worker = Process(target=worker, args=(1, "SQUARE")).start()

    # Create a "cube" worker
    cube_worker = Process(target=worker, args=(2, "CUBE")).start()

    # Fire up our result manager...
    result_manager = Process(target=result_manager, args=())
    result_manager.start()
    time.sleep(1)
    # Start the ventilator!
    work_addresses = ["CUBE", "SQUARE"]
    ventilator = Process(target=ventilator, args=(work_addresses,))
    ventilator.start()
