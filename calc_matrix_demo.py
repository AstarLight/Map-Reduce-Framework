# -*- coding: utf-8 -*-
# Author: lijunshi
# Date: 2018-06-18
# Email: lijunshi2015@163.com

import unittest
from map_reduce import MapReduceHandler
from job_descriptor import JobDescriptor

test_array = np.random.randint(10,size=10000000000)


class CalculatorService(MapReduceHandler):
    def __process_map(self, job):
        if job.get_field("name", None) != "server":
            newjob.set_field("name", "client")
            newjob.set_field("status", "-1")
            self.out_channel.emit_a_job(newjob)

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
            newjob.set_field("return_channel", reduce_in_channel)
            # if we can set job name as  "mini-calc"(all calc share 1 channel) or "mini-calc-x"(1 calc 1 channel)
            newjob.set_field("name", "mini-calc")
            newjob.set_field("status", "1")
            self.reduce_out_channel.emit_a_job(newjob)

    def __process_reduce(self, jobs):
        sum = 0
        for job in jobs:
            value = job.get_field("value", 0)
            sum += value
        newjob = JobDescriptor()
        newjob.set_field("result", sum)
        newjob.set_field("name", "client")
        self.out_channel.emit_a_job(newjob)


class TestMapReduce(unittest.TestCase):
    def SetUp(self):
        pass

    def test_mapreduce(self):
        pass

if __name__ == '__main__':
    unittest.main()