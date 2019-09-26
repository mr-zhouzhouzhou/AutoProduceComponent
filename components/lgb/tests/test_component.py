# Copyright 2018 Google LLC
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
#      http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import os
import subprocess
import tempfile
import unittest
from contextlib import contextmanager
from pathlib import Path
import findspark
findspark.init()
import pyspark.sql

import kfp.components as comp

@contextmanager
def components_local_output_dir_context(output_dir: str):
    old_dir = comp._components._outputs_dir
    try:
        comp._components._outputs_dir = output_dir
        yield output_dir
    finally:
        comp._components._outputs_dir = old_dir

class LgbTestCase(unittest.TestCase):

    def test_1(self):
        tests_root = os.path.abspath(os.path.dirname(__file__))
        component_root = os.path.abspath(os.path.join(tests_root, '..'))
        testdata_root = os.path.abspath(os.path.join(tests_root, 'testdata'))
        
        train_op = comp.load_component(os.path.join(component_root, 'component.yaml'))

        with tempfile.TemporaryDirectory() as temp_dir_name:
            with components_local_output_dir_context(temp_dir_name):
                train_task = train_op(
                    train_path="/tmp/automl_spark/8e091568-2a9f-4ce5-ac75-c4ffa61a7a20/pipeline-sm6jc-2569523609/train",
                    label_name='rating',
                    out_model_path="/tmp/automl_spark/train/ee.out",
                    k_flods= 3,
                    scoring= "accuracy"
                )

            full_command = train_task.command + train_task.arguments
            full_command[0] = 'python'
            full_command[1] = os.path.join(component_root, 'src', 'lgb.py')

            process = subprocess.run(full_command)

            (output_model_uri_file, ) = (train_task.file_outputs['out_model_path'], )
            output_model_uri = Path(output_model_uri_file).read_text()



    # def test_2(self):
    #     tests_root = os.path.abspath(os.path.dirname(__file__))
    #     sc = pyspark.sql.SparkSession.builder.master("local").getOrCreate()
    #
    #     x_train = os.path.abspath(os.path.join(tests_root, 'testdata/x_train.parquet'))
    #     y_train = os.path.abspath(os.path.join(tests_root, 'testdata/y_train.parquet'))
    #     x_test = os.path.abspath(os.path.join(tests_root, 'testdata/x_test.parquet'))
    #     y_test = os.path.abspath(os.path.join(tests_root, 'testdata/y_test.parquet'))
    #
    #
    #
    #     df = sc.read.parquet("file://" + x_train)
    #
    #     df.show()
    #
    #     df = sc.read.parquet("file://" + y_train)
    #
    #     df.show()
    #
    #     df = sc.read.parquet("file://" + x_test)
    #
    #     df.show()
    #
    #     df = sc.read.parquet("file://" + y_test)
    #
    #     df.show()


if __name__ == '__main__':
    unittest.main()
