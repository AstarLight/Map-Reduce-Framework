# -*- coding: utf-8 -*-
# Author: lijunshi
# Date: 2018-06-18
# Email: lijunshi2015@163.com

class JobDescriptor(object):
    def __init__(self, **kwargs):
        self.job = {}
        for k, v in kwargs.items():
            self.job[k] = v

    def set_field(self, k, v):
        self.job[k] = v
        return self

    def get_field(self, k):
        return self.job.get(k, default=None)