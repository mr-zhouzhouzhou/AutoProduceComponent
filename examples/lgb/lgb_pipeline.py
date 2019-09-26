#!/usr/bin/python python
# -*- coding: utf-8 -*-
# @File  : lgb_pipeline.py
# @Author: tony_zhou
# @Date  : 2019-09-08
#@license : Copyright(C), sense.ai
#@Contact : 1801210925@pku.edu.cn
#@Software : PyCharm
import sys
import kfp
from kfp import components
from kfp import dsl
from kubernetes import client as k8s_client


@dsl.pipeline(name='pipeline', description='test')
def ml_test(dataset_path="/benchmark_data/movielens_small/ratings",
            hash_buckets_size=100,
            out_path="/tmp/automl_spark/{{workflow.uid}}/{{pod.name}}/hash",
            entilty_list=str(["userId", "movieId"]),
            time_column_name="timestamp",
            time_to_columns=str(["week", "day"]),
            out_timestamp_path="/tmp/automl_spark/{{workflow.uid}}/{{pod.name}}/timestamp",

            out_label_path="/tmp/automl_spark/{{workflow.uid}}/{{pod.name}}/label",
            label_name="rating",
            test_size=0.2,
            out_train_path="/tmp/automl_spark/{{workflow.uid}}/{{pod.name}}/train",
            out_test_path="/tmp/automl_spark/{{workflow.uid}}/{{pod.name}}/test",
            k_flods=3,
            scoring="accuracy",
            out_model_path="/tmp/automl_spark/{{workflow.uid}}/{{pod.name}}/model.out"
            ):
    hash_yaml = kfp.components.load_component_from_file("yaml/hash.yaml")
    hash_op = hash_yaml(dataset_path, hash_buckets_size, out_path, entilty_list)

    time_yaml = kfp.components.load_component_from_file("yaml/timestamp.yaml")
    time_op = time_yaml(dataset_path, time_column_name, time_to_columns, out_timestamp_path)

    label_yaml = kfp.components.load_component_from_file("yaml/label.yaml")
    label_op = label_yaml(dataset_path, label_name, out_label_path)

    input_path_list = []
    input_path_list.append(hash_op.outputs['out_hash_path'])
    input_path_list.append(label_op.outputs['out_label_path'])
    input_path_list.append(time_op.outputs['out_timestamp_path'])
    bind_yaml = kfp.components.load_component_from_file("yaml/bind.yaml")
    bind_op = bind_yaml(str(input_path_list),
                        label_name,
                        test_size,
                        out_train_path,
                        out_test_path)

    lgb_yaml = kfp.components.load_component_from_file("yaml/lgb.yaml")
    lgb_op = lgb_yaml(bind_op.outputs["out_train_path"],
                      label_name,
                      out_model_path,
                      k_flods,
                      scoring)

    verification_yaml = kfp.components.load_component_from_file("yaml/ve.yaml")
    verification_op = verification_yaml(bind_op.outputs["out_test_path"],
                                        label_name,
                                        lgb_op.outputs["out_model_path"]
                                        )

    verification_op.after(lgb_op)


if __name__ == '__main__':
    kfp.compiler.Compiler().compile(ml_test, 'my-pipeline.zip')
    client = kfp.Client(host="10.98.51.118", namespace="kubeflow")
    my_experiment = client.create_experiment(name='zhouwang')
    my_run = client.run_pipeline(my_experiment.id, 'my-pipeline',
                                 'my-pipeline.zip')









