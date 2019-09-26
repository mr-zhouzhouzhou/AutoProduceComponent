#!/usr/bin/python python
# -*- coding: utf-8 -*-
# @File  : test_component.py
# @Author: tony_zhou
# @Date  : 2019-09-20
#@license : Copyright(C), sense.ai
#@Contact : 1801210925@pku.edu.cn
#@Software : PyCharm


import os
import subprocess
import random
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path

import kfp.components as comp

import findspark
findspark.init()
import pyspark.sql




@contextmanager
def components_local_output_dir_context(output_dir: str):
    old_dir = comp._components._outputs_dir

    try:
        comp._components._outputs_dir = output_dir
        yield output_dir
    finally:
        comp._components._outputs_dir = old_dir




class LabelTestCase(unittest.TestCase):


    def test_1(self):
        tests_root = os.path.abspath(os.path.dirname(__file__))
        component_root = os.path.abspath(os.path.join(tests_root, '..'))
        testdata_root = os.path.abspath(os.path.join(tests_root, 'testdata'))

        train_op = comp.load_component(os.path.join(component_root, 'component.yaml'))

        with tempfile.TemporaryDirectory() as temp_dir_name:
            with components_local_output_dir_context(temp_dir_name):
                train_task = train_op(
                    dataset_path="file://"+os.path.join(testdata_root,"rating.parquet"),
                    label_name = "rating",
                    out_label_path="file://"+os.path.join(testdata_root,"label.parquet")
                )

            full_command = train_task.command + train_task.arguments
            full_command[0] = 'python'
            full_command[1] = os.path.join(component_root, 'src', 'label_preprocess.py')

            process = subprocess.run(full_command)

            (output_model_uri_file,) = (train_task.file_outputs['out_label_path'],)

            output_model_uri_file = os.path.join(temp_dir_name,output_model_uri_file)


            output_model_uri = Path(output_model_uri_file).read_text()



    def test_2(self):
        tests_root = os.path.abspath(os.path.dirname(__file__))

        testdata_root = os.path.abspath(os.path.join(tests_root, 'testdata/label.parquet'))

        sc = pyspark.sql.SparkSession.builder.master("local").getOrCreate()

        df = sc.read.parquet("file://" + testdata_root)

        df.show()


if __name__ == '__main__':
    unittest.main()

