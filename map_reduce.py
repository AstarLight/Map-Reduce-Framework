# -*- coding: utf-8 -*-
# Author: lijunshi
# Date: 2018-06-18
# Email: lijunshi2015@163.com

import logging
from time import sleep
import multiprocessing as mp
import numpy as np
from abc import abstractmethod, ABCMeta
from standalone_channel import Standalone


class MapReduceHandler(object):

    def __init__(self):
        self.in_channel = None
        self.out_channel = None
        self.reduce_in_channel = None
        self.t = None
        self.services_channel_dict = {}
        self.workers_num = 0

    # map reduce core
    @abstractmethod
    def process_map(self, job):
        # 1. split a big job into several small tasks
        # 2. dispatch tasks to workers process/server
        # 3. wait for reduce result from workers(block)
        # 4. collect all sub results
        # 5. return final result
        raise NotImplementedError

    @abstractmethod
    def process_reduce(self, jobs):
        raise NotImplementedError

    def accept(self):
        logging.info("------ Map-Reduce Service Start ------")
        while True:
            job = self.in_channel.pull_a_job()
            if job is not None:
                logging.debug("Map-Reduce main Service receives a job: %s", job.to_json_str())
                count = self.process_map(job)
                if count == 0:
                    continue
                reduce_jobs = []
                current_count = 0
                while True:
                    ret_job = self.reduce_in_channel.pull_a_job()
                    if ret_job is not None:
                        logging.debug("Reduce Service receives a job: %s", ret_job.to_json_str())
                        reduce_jobs.append(ret_job)
                        current_count +=1
                        if count == current_count:
                            logging.debug("We receive all reduce jobs!")
                            self.process_reduce(reduce_jobs)
                            break
                    else:
                        sleep(0.05)
            else:
                sleep(0.05)

    def register(self, service):
        if service.name not in self.services_channel_dict:
            self.services_channel_dict[service.name] = Standalone()
            self.services_channel_dict[service.name].init_channel()
        service.in_channel = self.services_channel_dict[service.name]

    def run(self):
        self.t = mp.Process(target=MapReduceHandler.accept, args=(self,))
        self.t.daemon = True
        self.t.start()



