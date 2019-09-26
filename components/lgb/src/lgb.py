#!/usr/bin/python python
# -*- coding: utf-8 -*-
# @File  : verification_test.py
# @Author: tony_zhou
# @Date  : 2019-09-05
#@license : Copyright(C), sense.ai
#@Contact : 1801210925@pku.edu.cn
#@Software : PyCharm

import os
import argparse
import logging
import findspark
import shlex, subprocess

from pathlib import Path
findspark.init()
import pyspark.sql
from lightgbm import LGBMClassifier
from sklearn.externals import joblib
from sklearn.model_selection import cross_val_score



class Train_GBM:

    def __init__(self, train_path, label_name,out_model_path , k_flods, scoring,out_model_path_file):

        self.train_path = train_path
        self.label_name = label_name
        self.out_model_path = out_model_path
        self.k_flods = k_flods
        self.scoring = scoring
        self.out_model_path_file = out_model_path_file
        subprocess.call("echo  192.168.10.145 ab-node1.senses >> /etc/hosts", shell=True)
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.sc = pyspark.sql.SparkSession.builder.master("local").getOrCreate()



    def popen(self,commod):

        proc = subprocess.Popen(commod,shell=True)
        try:
            outs, errs = proc.communicate()
        except subprocess.TimeoutExpired:
            proc.kill()
            outs, errs = proc.communicate()


    def train(self):

        train_df = self.sc.read.parquet(self.train_path)

        x_df = train_df.drop("index", self.label_name)
        y_df = train_df.select(self.label_name)

        x_train_list = [[row[item] for item in x_df.columns] for row in x_df.collect()]

        y_train_list = [row[item] for item in y_df.columns for row in y_df.collect()]

        lgbm = LGBMClassifier(boosting_type='gbdt',
                              num_leaves=31,
                              max_depth=-1,
                              learning_rate=0.1,
                              n_estimators=100,
                              subsample_for_bin=200000)

        clf = lgbm.fit(x_train_list, y_train_list)

        scores = cross_val_score(clf, x_train_list, y_train_list, cv=3, scoring=self.scoring)

        self.logger.info(f'{self.k_flods}次的预测准确率:\n, {scores}')
        self.logger.info(f'平均预测准确率:\n, {scores.mean()}')


        file_name = os.path.basename(self.out_model_path)

        dir_path = os.path.dirname(self.out_model_path)


        local_model_path = os.path.join("/home/",file_name)
        joblib.dump(clf, local_model_path)

        os.path.dirname(self.out_model_path)



        self.popen([f"hdfs dfs -mkdir -p {dir_path}"])

        self.popen([f"hdfs dfs -put {local_model_path} {dir_path}"])

        self.logger.info(f'The model has saved in :\n, {self.out_model_path}')
        Path(self.out_model_path_file).parent.mkdir(parents=True, exist_ok=True)
        Path(self.out_model_path_file).write_text(self.out_model_path)


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--train-path',         type=str, required=True, help='the path of dataset')
    parser.add_argument('--label-name',         type=str, required=True, help='the path of preprocessed dataset will be saved')
    parser.add_argument('--out-model-path',     type=str, required=True, help='the label you want to preprocess')
    parser.add_argument('--k-flods',            type=int, required=True, help='the path of preprocessed dataset will be saved')
    parser.add_argument('--scoring',            type=str, required=True, help='the type of ...')
    parser.add_argument('--out-model-path-file',type=str, required=True, help='the type of ...')
    args = parser.parse_args()

    Train_GBM(train_path=args.train_path,
              label_name=args.label_name,
              out_model_path=args.out_model_path,
              k_flods=args.k_flods,
              scoring=args.scoring,
              out_model_path_file=args.out_model_path_file
              ).train()


if __name__ == "__main__":
    main()


