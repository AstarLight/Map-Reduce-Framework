# -*- coding: utf-8 -*-
# Author: lijunshi
# Date: 2018-06-18
# Email: lijunshi2015@163.com

class Worker(object):
    def __init__(self):
        self.in_channel = None
        self.out_channel = None
        self.t = None

    @abstractmethod
    def work(self, job):
        raise NotImplementedError

    def accept(self):
        logging.info("------ Mini-Calc Service Start ------")
        while True:
            job = self.in_channel.pull_a_job()
            if job is not None:
                work(job)
            else:
                sleep(0.05)

    def run(self):
        self.t = mp.Process(target=Worker.accept, args=(self,))
        self.t.daemon = True
        self.t.start()