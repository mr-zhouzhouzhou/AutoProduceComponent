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

class BindTestCase(unittest.TestCase):

    def test_1(self):
        tests_root = os.path.abspath(os.path.dirname(__file__))
        component_root = os.path.abspath(os.path.join(tests_root, '..'))
        testdata_root = os.path.abspath(os.path.join(tests_root, 'testdata'))
        
        train_op = comp.load_component(os.path.join(component_root, 'component.yaml'))

        input_path_list = [{'PipelineParam': {'name': 'out_path', 'op_name': 'Spark Hash Preprocess', 'value': None, 'param_type': 'List',
                            'pattern': '/tmp/automl_spark/7e4b8111-f20b-4991-a3b4-90d6a0ce3389/pipeline-hbbsb-1543833624-hash'}},
         {'PipelineParam': {'name': 'out_label_path', 'op_name': 'Spark Label Preprocess', 'value': None,
                            'param_type': 'String',
                            'pattern': '/tmp/automl_spark/7e4b8111-f20b-4991-a3b4-90d6a0ce3389/pipeline-hbbsb-1645838448-label'}},
         {'PipelineParam': {'name': 'out_timestamp_path', 'op_name': 'Spark Timestamp Prepeocess', 'value': None,
                            'param_type': 'String',
                            'pattern': '/tmp/automl_spark/7e4b8111-f20b-4991-a3b4-90d6a0ce3389/pipeline-hbbsb-1323756077-timestamp'}}]

        tmp = ["file://" + os.path.join(testdata_root, "label.parquet"),
         "file://" + os.path.join(testdata_root, "time.parquet")]

        with tempfile.TemporaryDirectory() as temp_dir_name:
            with components_local_output_dir_context(temp_dir_name):
                train_task = train_op(
                    input_path_list=input_path_list,
                    label_name='rating',
                    test_size=0.8,
                    out_train_path = "file://"+os.path.join(testdata_root,"train.parquet"),
                    out_test_path= "file://"+os.path.join(testdata_root, "test.parquet")
                )

            full_command = train_task.command + train_task.arguments
            full_command[0] = 'python'
            full_command[1] = os.path.join(component_root, 'src', 'bind_preprocess.py')

            process = subprocess.run(full_command)

            (output_model_uri_file, ) = (train_task.file_outputs['out_train_path'], )
            output_model_uri = Path(output_model_uri_file).read_text()

            (output_model_uri_file,) = (train_task.file_outputs['out_test_path'],)
            output_model_uri = Path(output_model_uri_file).read_text()




    def test_2(self):
        tests_root = os.path.abspath(os.path.dirname(__file__))
        sc = pyspark.sql.SparkSession.builder.master("local").getOrCreate()

        train_path = os.path.abspath(os.path.join(tests_root, 'testdata/train.parquet'))
        test_path = os.path.abspath(os.path.join(tests_root, 'testdata/test.parquet'))




        df = sc.read.parquet("file://" + train_path)

        df.show()

        df = sc.read.parquet("file://" + test_path)

        df.show()

if __name__ == '__main__':
    unittest.main()
