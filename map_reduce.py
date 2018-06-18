# -*- coding: utf-8 -*-
# Author: lijunshi
# Date: 2018-06-18
# Email: lijunshi2015@163.com

import logging
from time import sleep
import multiprocessing as mp

class MapReduceHandler(object):
    self.in_channel
    self.out_channel
    self.reduce_in_channel
    self.reduce_out_channel
    self.t = None
    self.services_channel_dict = {}

    def __dispatch(self, task):


    # map reduce core
    def process(self, job):
        # 1. split a big job into several small tasks
        # 2. dispatch tasks to workers process/server
        __dispatch(job)
        # 3. wait for reduce result from workers(block)
        # 4. collect all sub results
        # 5. return final result

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


