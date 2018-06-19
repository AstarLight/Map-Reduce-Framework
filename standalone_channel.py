# -*- coding: utf-8 -*-
# Author: lijunshi
# Date: 2018-06-18
# Email: lijunshi2015@163.com

''' standalone communication mode(IPC) '''

from channel import channel
import multiprocessing as mp

class Standalone(Channel):
    def __init__(self):
        self.q = None

    def init_channel(self):
        if self.q is None:
            self.q = mp.Queue()

    def shutdown_channel(self):
        pass

    def pull_a_job(self):
        try:
            return self.q.get()
        except Exception:
            return None

    def emit_a_job(self, job):
        try:
            self.q.put(job)
            return True
        except Exception:
            return False


