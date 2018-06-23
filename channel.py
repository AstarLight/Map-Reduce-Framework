# -*- coding: utf-8 -*-
# Author: lijunshi
# Date: 2018-06-18
# Email: lijunshi2015@163.com

from abc import abstractmethod, ABCMeta

class Channel(object):
    @abstractmethod
    def init_channel(self):
        raise NotImplementedError

    @abstractmethod
    def shutdown_channel(self):
        raise NotImplementedError

    @abstractmethod
    def pull_a_job(self):
        raise NotImplementedError

    @abstractmethod
    def emit_a_job(self, job):
        raise NotImplementedError