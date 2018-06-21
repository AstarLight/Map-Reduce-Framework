# -*- coding: utf-8 -*-
# Author: lijunshi
# Date: 2018-06-18
# Email: lijunshi2015@163.com

import unittest
from map_reduce import MapReduceHandler
from job_descriptor import JobDescriptor
from worker import Worker
from standalone_channel import Standalone
import logging
import time

logging.basicConfig(level=logging.DEBUG)

test_array = np.random.randint(10,size=10000000000)
workers_num = 5


class CalculatorService(MapReduceHandler):
    def __process_map(self, job):
        if job.get_field("name", None) != "server":
            newjob = JobDescriptor()
            newjob.set_field("name", "client")
            newjob.set_field("status", "-1")
            self.out_channel.emit_a_job(newjob)
            logging.debug("CalculatorService emits a job: %s", newjob.to_json_str())
            return 0
        else:
            self.out_channel = job.get_field("return_channel", None)
            size = job.get_field('array_size', 0)
            worker_num = len(self.services_channel_dict)
            sub_size = size / worker_num
            start = 0
            end = 0
            for idx in range(worker_num):
                start = end
                if idx == worker_num -1:
                    end = size - 1
                else:
                    end = end + sub_size
                newjob = JobDescriptor()
                newjob.set_field("start", start)
                newjob.set_field("end", end)
                newjob.set_field("return_channel", self.reduce_in_channel)
                # we can set job name as  "mini-calc"(all calc share 1 channel) or "mini-calc-x"(1 calc 1 channel)
                newjob.set_field("name", "mini-calc")
                newjob.set_field("status", "1")
                self.reduce_out_channel.emit_a_job(newjob)
                logging.debug("CalculatorService emits a job: %s", newjob.to_json_str())
                return worker_num

    def __process_reduce(self, jobs):
        sum = 0
        for job in jobs:
            value = job.get_field("value", 0)
            sum += value
        newjob = JobDescriptor()
        newjob.set_field("result", sum)
        newjob.set_field("name", "client")
        self.out_channel.emit_a_job(newjob)

class MiniCalcWorker(Worker):
    def work(self, job):
        newjob = JobDescriptor()
        if job.get_field("name", None) != "mini-calc":
            self.out_channel = job.get_field("return_channel", None)
            newjob.set_field("name", "server")
            newjob.set_field("status", "-1")
            self.out_channel.emit_a_job(newjob)
        else:
            self.out_channel = job.get_field("return_channel", None)
            start = job.get_field("start", 0)
            end = job.get_field("end", 0)
            new_array = test_array[start:end]
            sum = np.sum(new_array)
            newjob.set_field("value", sum)
            newjob.set_field("name", "server")
            newjob.set_field("status", "1")
        self.out_channel.emit_a_job(newjob)
        logging.debug("MiniCalcWorker emits a job: %s", newjob.to_json_str())

class Client(unittest.TestCase):
    def __init__(self):
        self.send_channel = None
        self.receive_channel = None
        self.serial_sum = 0
        self.t = None

    def check(self, job):
        return_name = job.get_field("name", None)
        self.assertEqual(return_name, "client")
        calc_status = job.get_field("status", -1)
        self.assertEqual(calc_status, "1")
        final_sum = job.get_field("result", -1)
        self.assertEqual(final_sum, self.serial_sum)

    def process(self):
        t1 = time.time()
        self.serial_sum = np.sum(test_array)
        logging.debug("serial calculation time: %d", time.time()-t1)
        t1 = time.time()
        newjob = JobDescriptor()
        newjob.set_field("array_size", len(test_array))
        newjob.set_field("name", "server")
        self.send_channel.emit_a_job(newjob)
        logging.debug("Client emits a job: %s", newjob.to_json_str())
        while True:
            job = self.receive_channel.pull_a_job()
            logging.debug("Client receives a job: %s", job.to_json_str())
            if job is not None:
                check(job)
                break
            else:
                sleep(0.05)

        logging.debug("parallel calculation time: %d", time.time() - t1)

    def run(self):
        self.t = mp.Process(target=Client.process, args=(self,))
        self.t.daemon = True
        self.t.start()

class TestMapReduce(unittest.TestCase):

    def test_mapreduce(self):
        client_out_channel = Standalone()
        client_out_channel.init_channel()
        client_in_channel = Standalone()
        client_in_channel.init_channel()

        calculator_agent = CalculatorService()
        calculator_agent.out_channel = client_in_channel
        calculator_agent.in_channel = client_out_channel

        for i in range(workers_num):
            worker = MiniCalcWorker("mini-calc")
            calculator_agent.register(worker)
            worker.run()

        calculator_agent.run()

        test_client = Client()
        test_client.send_channel = client_out_channel
        test_client.receive_channel = client_in_channel

        test_client.run()
        test_client.t.join()


if __name__ == '__main__':
    unittest.main()