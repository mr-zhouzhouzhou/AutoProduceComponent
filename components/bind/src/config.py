#!/usr/bin/python python
# -*- coding: utf-8 -*-
# @File  : config.py
# @Author: tony_zhou
# @Date  : 2019-09-27
#@license : Copyright(C), sense.ai
#@Contact : 1801210925@pku.edu.cn
#@Software : PyCharm



def get_config():
    return {"input":["input-path-list",
                     "label-name",
                     "test-size",
                     "out-train-path",
                     "out-test-path",
                     "out-train-path-file",
                     "out-test-path-file"],
            "output":{"out-train-path-file":"out-train-path",
                      "out-test-path-file":"out-test-path"}}

