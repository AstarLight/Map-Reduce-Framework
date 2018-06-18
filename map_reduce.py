# -*- coding: utf-8 -*-
# Author: lijunshi
# Date: 2018-06-18
# Email: lijunshi2015@163.com

import logging
from time import sleep
import multiprocessing as mp
import numpy as np

worker_num = 4
test_matrix = np.random.randint(1,10,size=(worker_num,1000000))

class MapReduceHandler(object):
    def __init__(self):
        self.in_channel
        self.out_channel
        self.reduce_in_channel
        self.reduce_out_channel
        self.t = None
        self.services_channel_dict = {}

    def __dispatch(self, task):
        for key in self.services_channel_dict:
            self.services_channel_dict[key].emit_a_job(task)

    # map reduce core
    def process(self, job):
        # 1. split a big job into several small tasks
        tasks_list = []
        for i in range(test_matrix.cols):
            tasks_list.append(test_matrix[i])
        total_sub_result_count = len(self.services_channel_dict)
        # 2. dispatch tasks to workers process/server
        for task in tasks_list:
            __dispatch(task)
        # 3. wait for reduce result from workers(block)
        is_calc_ok = True
        final_result = 0
        while True:
            ret_job = self.reduce_in_channel.pull_a_job()
            if ret_job is not None:
                sub_result = ret_job.get_field("result", -1)
                if sub_result == -1:
                    is_calc_ok = False
                    break
                count += 1
                # 4. collect all sub results
                final_result += sub_result
                if count == total_sub_result_count:
                    break
            else:
                sleep(0.05)
        # 5. return final result
        return (final_result, isCalcOK)

    def accept(self):
        logging.info("------ Map-Reduce Service Start ------")
        while True:
            job = self.in_channel.pull_a_job()
            if job is not None:
                logging.debug("Map-Reduce Service receives a job : %s", job.name)
                self.process(job)
            else:
                sleep(0.05)


    def register(self, service):
        if service.name not in self.services_channel_dict:
            self.services_channel_dict[service.name] = service
            self.services_channel_dict[service.name].in_channel.init()
        service.in_channel = self.services_channel_dict[service.name]

    def run(self):
        self.t = mp.Process(target=MapReduceHandler.accept, args=(self,))
        self.t.daemon = True
        self.t.start()


