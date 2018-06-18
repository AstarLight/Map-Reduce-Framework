# -*- coding: utf-8 -*-
# Author: lijunshi
# Date: 2018-06-18
# Email: lijunshi2015@163.com

class Channel(object):

    @abstractmethod
    def init_channel(self):
        pass

    @abstractmethod
    def shutdown_channel(self):
        pass

    @abstractmethod
    def pull_a_job(self):
        pass

    @abstractmethod
    def emit_a_job(self, job):
        pass