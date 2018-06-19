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
    @abstractmethod
    def __process_map(self, job):
        map_count = 0
        # 1. split a big job into several small tasks
        # 2. dispatch tasks to workers process/server
        # 3. wait for reduce result from workers(block)
        # 4. collect all sub results
        # 5. return final result
        return map_count

    @abstractmethod
    def __process_reduce(self, jobs):
        pass

    def __accept(self):
        logging.info("------ Map-Reduce Service Start ------")
        while True:
            job = self.in_channel.pull_a_job()
            if job is not None:
                logging.debug("Map-Reduce Service receives a job : %s", job.name)
                count = self.__process_map(job)
                reduce_jobs = {}
                current_count = 0
                while True:
                    ret_job = self.reduce_in_channel.pull_a_job()
                    if ret_job is not None:
                        reduce_jobs.append(ret_job)
                        current_count +=1
                        if count == current_count:
                            logging.debug("We receive all reduce jobs!")
                            process_reduce(reduce_jobs)
                    else:
                        sleep(0.05)
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


