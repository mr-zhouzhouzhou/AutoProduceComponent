#!/usr/bin/python python
# -*- coding: utf-8 -*-
# @File  : yaml.py
# @Author: tony_zhou
# @Date  : 2019-09-26
#@license : Copyright(C), sense.ai
#@Contact : 1801210925@pku.edu.cn
#@Software : PyCharm

import os
from ruamel import yaml


desired_caps = {
	"name": "Spark Hash Preprocess",
	"inputs": [
		{
			"name": "dataset_path",

		},
		{
			"name": "hash_buckets_size",

		},
		{
			"name": "out_hash_path",

		},
		{
			"name": "entilty_list",
		}
	],
	"outputs": [
		{
			"name": "out_hash_path",
			"type": "List",
			"description": "the out path of you wang to save"
		}
	],
	"implementation": {
		"container": {
			"image": "hash:default",
			"command": [
				"python",
				"hash.py"
			],
			"args": [
				"--dataset-path",
				{
					"inputValue": "dataset_path"
				},
				"--hash-buckets-size",
				{
					"inputValue": "hash_buckets_size"
				},
				"--out-hash-path",
				{
					"inputValue": "out_hash_path"
				},
				"--entilty-list",
				{
					"inputValue": "entilty_list"
				},
				"--out-path-file",
				{
					"outputPath": "out_hash_path"
				}
			]
		}
	}
}

# curpath = os.path.dirname(os.path.realpath(__file__))
# yamlpath = os.path.join(curpath, "cap11s.yaml")
#
# # 写入到yaml文件
# with open(yamlpath, "w", encoding="utf-8") as f:
#    yaml.dump(desired_caps, f, Dumper=yaml.RoundTripDumper)




yaml_path = "cap11s.yaml"

a = open(yaml_path,'r')

yaml.load(a.read(),Loader=yaml.Loader)