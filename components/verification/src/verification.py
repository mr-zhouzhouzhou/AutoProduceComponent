#!/usr/bin/python python
# -*- coding: utf-8 -*-
# @File  : verification.py
# @Author: tony_zhou
# @Date  : 2019-09-08
#@license : Copyright(C), sense.ai
#@Contact : 1801210925@pku.edu.cn
#@Software : PyCharm

import pandas as pd
import os
import argparse
import logging
import  subprocess
from sklearn.externals import joblib
from sklearn import metrics
import findspark
findspark.init()

import pyspark.sql


class Verification:

    def __init__(self, test_path,label_name, save_model_path):
        """

        Parameters
        ----------
        test_path :  the path of the tests(x)
        label_name :  the path of the tests(label)
        save_model_path : the path of the model


        example:
                x_test_path = '/home/testdata/lgb/x_test.csv'
                y_test_path = '/home/testdata/lgb/y_test.csv'
                save_path_model = '/home/testdata/lgb/train_model.m'
                Verification(x_test_path=x_test_path,
                             y_test_path=y_test_path,
                             save_path_model=save_path_model
                             ).tests()


        """


        self.model_name = os.path.basename(save_model_path)
        self.test_path = test_path
        self.label_name = label_name
        self.save_path_model = save_model_path

        self.sc = pyspark.sql.SparkSession.builder.master("local").getOrCreate()

        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        subprocess.call("echo  192.168.10.145 ab-node1.senses >> /etc/hosts",shell=True)



    def popon(self,commod,is_shell):

        proc = subprocess.Popen(commod,shell=is_shell)
        try:
            outs, errs = proc.communicate(timeout=15)
        except subprocess.TimeoutExpired:
            proc.kill()
            outs, errs = proc.communicate()

    def load(self):




        self.popon(["hdfs","dfs","-get",self.save_path_model, "/home"],is_shell=False)


        self.model_path = os.path.join("/home",self.model_name)


        self.clf = joblib.load(self.model_path)


    def test(self):
        test_df = self.sc.read.parquet(self.test_path)

        x_df = test_df.drop("index", self.label_name)
        y_df = test_df.select(self.label_name)

        x_test = [[row[item] for item in x_df.columns] for row in x_df.collect()]

        y_test = [row[item] for item in y_df.columns for row in y_df.collect()]

        self.load()

        y_predict = self.clf.predict(x_test)
        fpr, tpr, thresholds = metrics.roc_curve(y_test, y_predict, pos_label=1)
        accuracy_score = metrics.accuracy_score(y_test, y_predict)
        self.logger.info(f"accuracy_score is {accuracy_score}")
        auc = metrics.auc(fpr, tpr)
        logging.info(f"auc is {auc}")
        roc_auc_score = metrics.roc_auc_score(y_test, y_predict)
        self.logger.info(f"roc_auc_score  is {roc_auc_score}")



def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('--test-path', type=str, required=True, help='the path of verification_input')
    parser.add_argument('--label-name', type=str, required=True,
                        help='the path of verification_label')
    parser.add_argument('--save-model-path', type=str, required=True, help='the path of the model')
    args = parser.parse_args()
    Verification(args.test_path,
                   args.label_name,
                   args.save_model_path).test()



if __name__ == "__main__":

    main()
    # test_path = "/tmp/automl_spark/e08ba0f8-559e-4fc8-bba7-cd1aea519996/pipeline-kls7n-3358759025-test",
    # label_name = "rating",
    # save_model_path = "/tmp/automl_spark/e08ba0f8-559e-4fc8-bba7-cd1aea519996/pipeline-kls7n-4073170716/model.out"
    #
    # Verification(test_path=test_path,
    #              label_name=label_name,
    #              save_model_path=save_model_path
    #              ).test()





