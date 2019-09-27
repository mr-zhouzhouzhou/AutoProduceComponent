#!/usr/bin/python python
# -*- coding: utf-8 -*-
# @File  : test.py
# @Author: tony_zhou
# @Date  : 2019-09-26
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

set_input_out = get_config()
input_list = set_input_out["input"]
output_list = set_input_out["output"]

inputs = []
outputs = []
args = []

for item in input_list:
        if item not in output_list.keys():
            inputs.append({"name":item.replace("-","_")})
            args.append("--"+item)
            args.append({"inputValue":item.replace("-","_")})


for item in output_list:
    outputs.append({"name":output_list[item]})


for item in output_list:
       # outputs.append({"name":value.replace("-","_")})
        args.append("--" + item)
        args.append({"outputPath": output_list[item].replace("-", "_")})



print(args)
# print(outputs)
#
