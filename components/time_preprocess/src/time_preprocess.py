#!/usr/bin/python python
# -*- coding: utf-8 -*-
# @File  : time_preprocess.py
# @Author: tony_zhou
# @Date  : 2019-09-08
#@license : Copyright(C), sense.ai
#@Contact : 1801210925@pku.edu.cn
#@Software : PyCharm



import os
from pathlib import Path
import argparse
import subprocess
from datetime import datetime
import findspark
findspark.init()
import pyspark.sql
from pyspark.sql.functions import udf
from pyspark.sql.types import IntegerType
from pyspark.sql.functions import monotonically_increasing_id




class TimePreprocess:

    def __init__(self, dataset_path,time_column_name,time_to_columns,out_timestamp_path,out_timestamp_path_file):

        self.dataset_path = dataset_path
        self.time_column_name = time_column_name
        self.time_to_columns = time_to_columns
        self.out_timestamp_path = out_timestamp_path
        self.out_timestamp_path_file = out_timestamp_path_file
        subprocess.call("echo  192.168.10.145 ab-node1.senses >> /etc/hosts", shell=True)

    def time_transform(self, timestamp, time_type):

        t = datetime.strptime(timestamp, '%Y-%m-%d %H:%M:%S')
        if time_type == "week":
            return int(t.strftime("%w"))
        if time_type == "year":
            return int(t.year)
        if time_type == "month":
            return int(t.month)
        if time_type == "day":
            return int(t.day)
        if time_type == "hour":
            return int(t.hour)
        if time_type == "minute":
            return int(t.minute)
        if time_type == "second":
            return int(t.second)

    def preprocess(self):

        """

        :return:
        example:
                 dataset_path = "/benchmark_data/movielens_small/ratings"
                 time_column_name = 'timestamp'
                 time_to_columns = ['month', 'day', 'week']
                 out_timestamp_path = "/home/testdata/lgb"
                 out_timestamp_path_file = "/home/testdata/lgb"

                 TimePreprocess(dataset_path=dataset_path,
                         time_column_name=time_column_name,
                         time_to_columns=time_to_columns,
                         out_timestamp_path=out_timestamp_path,
                         out_timestamp_path_file=out_timestamp_path_file).preprocess()
        """
        sc = pyspark.sql.SparkSession.builder.master("local").getOrCreate()

        df = sc.read.parquet(self.dataset_path).select(self.time_column_name)

        time_list = []
        time_list.append(monotonically_increasing_id().alias('index'))


        for item in self.time_to_columns:
            example_udf = udf(lambda x: self.time_transform(x, item), IntegerType())
            time_list.append(example_udf(self.time_column_name).alias(item))

        time_df = df.select(time_list)
        time_df.write.parquet(self.out_timestamp_path,mode="overwrite")

        Path(self.out_timestamp_path_file).parent.mkdir(parents=True, exist_ok=True)
        Path(self.out_timestamp_path_file).write_text(self.out_timestamp_path)






def main():

    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset-path',       type=str, required=True, help='the path of dataset')
    parser.add_argument('--time-column-name',   type=str, required=True, help='types ,you want to preprocess to transform the column of time to')
    parser.add_argument('--time-to-columns',    type=str, required=True, help='the column name of the timestamp ')
    parser.add_argument('--out-timestamp-path', type=str, required=True, help='the path of preprocessed dataset will be saved')
    parser.add_argument('--out-timestamp-path-file', type=str, required=True, help='the path where the trained model has been saved. ')
    args = parser.parse_args()
    TimePreprocess(args.dataset_path,
                   args.time_column_name,
                   eval(args.time_to_columns),
                   args.out_timestamp_path,
                   args.out_timestamp_path_file).preprocess()


if __name__ == "__main__":
    main()


