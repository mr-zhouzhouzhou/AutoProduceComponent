#!/usr/bin/python python
# -*- coding: utf-8 -*-
# @File  : bind_preprocess.py
# @Author: tony_zhou
# @Date  : 2019-09-05
#@license : Copyright(C), sense.ai
#@Contact : 1801210925@pku.edu.cn
#@Software : PyCharm


import logging
import subprocess
from pathlib import Path
import argparse
import findspark

findspark.init()
import pyspark.sql



class BindPreprocess:
    """
        input_path_list:  the file path of you want to bind(join)
        label_name: the label name
        test_size: the path you want to save your files
        out_train_path: the path of train dataset will be saved
        out_test_path: the path of test dataset will be saved
        out_train_path_file:...
        out_test_path_file:...


        ...
    """
    def __init__(self,
                 input_path_list,
                 label_name,
                 test_size,
                 out_train_path,
                 out_test_path,
                 out_train_path_file,
                 out_test_path_file
                 ):
        self.input_path_list = [input_path_list[index]["PipelineParam"]["pattern"] for index, item in enumerate(input_path_list)]
        self.label_name = label_name
        self.test_size = test_size
        self.out_train_path = out_train_path
        self.out_test_path = out_test_path
        self.out_train_path_file = out_train_path_file
        self.out_test_path_file = out_test_path_file

        self.seed = 1928

        self.popon("echo  192.168.10.145 ab-node1.senses >> /etc/hosts", is_shell=True)

        self.sc = pyspark.sql.SparkSession.builder.master("local").getOrCreate()

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)



    def popon(self, commod, is_shell):


        proc = subprocess.Popen(commod, shell=is_shell)
        try:
            outs, errs = proc.communicate()
        except subprocess.TimeoutExpired:
            proc.kill()
            outs, errs = proc.communicate()



    ###### add index column
    def get_dataframe(self, file_path):

        m_list = []
        #m_list.append(monotonically_increasing_id().alias('index'))

        df = self.sc.read.parquet(file_path)

        for item in df.columns:
            m_list.append(item)
        return df.select(m_list)



    def preprocess(self):

        df = self.get_dataframe(self.input_path_list[0])
        for item in self.input_path_list[1:]:
            temp_df = self.get_dataframe(item)
            df = df.join(temp_df, ['index'], "left_outer")



        weights = [1-self.test_size, self.test_size]
        train_df, test_df = df.randomSplit(weights, self.seed)

        train_df.write.parquet(self.out_train_path, mode="overwrite")
        test_df.write.parquet(self.out_test_path, mode="overwrite")
        self.logger.info(f"train dataset  has saved in  {self.out_train_path}")
        self.logger.info(f"test dataset  has saved in  {self.out_test_path}")

        file_list = [self.out_train_path, self.out_test_path]

        Path(self.out_train_path_file).parent.mkdir(parents=True, exist_ok=True)
        Path(self.out_train_path_file).write_text(self.out_train_path)


        Path(self.out_test_path_file).parent.mkdir(parents=True, exist_ok=True)
        Path(self.out_test_path_file).write_text(self.out_test_path)



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--input-path-list', type=str, required=True, help='the list of file path')
    parser.add_argument('--label-name', type=str, required=True, help='the label name')

    parser.add_argument('--test-size', type=float, required=True, help='the size of test dataset you want ')
    parser.add_argument('--out-train-path', type=str, required=True, help='the path of preprocessed train dataset will be saved')

    parser.add_argument('--out-test-path', type=str, required=True, help='the path of preprocessed test dataset will be saved')

    parser.add_argument('--out-train-path-file', type=str, required=True, help='...')

    parser.add_argument('--out-test-path-file', type=str, required=True, help='...')

    args = parser.parse_args()

    BindPreprocess(eval(args.input_path_list),
                   args.label_name,
                   args.test_size,
                   args.out_train_path,
                   args.out_test_path,
                   args.out_train_path_file,
                   args.out_test_path_file
                   ).preprocess()


if __name__ == "__main__":
    main()


