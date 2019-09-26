#!/usr/bin/python python
# -*- coding: utf-8 -*-
# @File  : label_preprocess.py
# @Author: tony_zhou
# @Date  : 2019-09-05
#@license : Copyright(C), sense.ai
#@Contact : 1801210925@pku.edu.cn
#@Software : PyCharm

import os
from pathlib import Path
import logging
import argparse
import findspark
import subprocess
findspark.init()
from pyspark.sql import SparkSession
from pyspark.sql.types   import IntegerType
from pyspark.sql.functions import udf
from pyspark.sql.functions import monotonically_increasing_id






class LabelPreprocess:

    def __init__(self, dataset_path, label_name, out_label_path,out_label_path_file):
        """

        Parameters
        ----------
        data_hdfs_path : the path of the dataset of train
        label_column : the column name of label
        save_path_label : the path of the label file
        """
        self.dataset_path = dataset_path
        self.label_name = label_name
        self.out_label_path = out_label_path
        self.out_label_path_file = out_label_path_file

        subprocess.call("echo  192.168.10.145 ab-node1.senses >> /etc/hosts", shell=True)

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)



    def label_udf(self, rating):

        if int(rating) > 3:
            return 1
        else:
            return 0



    def preprocess(self):
        """

        :return:
        Exaample:

             data_hdfs_path = "/benchmark_data/movielens_small/ratings"
             label_column = 'rating'
             save_path_label = '/home/testdata'
             LabelPreprocess(data_hdfs_path=data_hdfs_path,
                             label_column=label_column,
                             save_path_label=save_path_label
                             ).preprocess()
        """

        sc = SparkSession.builder.master("local").getOrCreate()
        label_udf = udf(self.label_udf, returnType=IntegerType())
        df = sc.read.parquet(self.dataset_path)
        label_df = df.select(monotonically_increasing_id().alias('index'),label_udf(self.label_name).alias(self.label_name))

        label_df.write.parquet(self.out_label_path,mode="overwrite")

        self.logger.info(f" label file  has  saved in {self.out_label_path}")

        Path(self.out_label_path_file).parent.mkdir(parents=True, exist_ok=True)
        Path(self.out_label_path_file).write_text(self.out_label_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--dataset-path', type=str, required=True, help='the path of dataset')
    parser.add_argument('--label-name', type=str, required=True,help='the name of column')
    parser.add_argument('--out-label-path', type=str, required=True, help='the label you want to save')
    parser.add_argument('--out-label-path-file', type=str, required=True, help='the label you want to save')
    args = parser.parse_args()
    LabelPreprocess(dataset_path=args.dataset_path,
                    label_name=args.label_name,
                    out_label_path=args.out_label_path,
                    out_label_path_file=args.out_label_path_file
                    ).preprocess()


if __name__ == "__main__":
    main()

