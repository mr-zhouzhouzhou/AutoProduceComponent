#!/usr/bin/python python
# -*- coding: utf-8 -*-
# @File  : test.py
# @Author: tony_zhou
# @Date  : 2019-09-26
#@license : Copyright(C), sense.ai
#@Contact : 1801210925@pku.edu.cn
#@Software : PyCharm


import importlib
aa = importlib.import_module('lib.aa')
add_print = aa.add_print()
print(add_print)
