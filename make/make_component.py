#!/usr/bin/python python
# -*- coding: utf-8 -*-
# @File  : make_component.py
# @Author: tony_zhou
# @Date  : 2019-09-26
#@license : Copyright(C), sense.ai
#@Contact : 1801210925@pku.edu.cn
#@Software : PyCharm


import os
import  subprocess



def mkdir_add_init(component_path):

        if  os.path.exists(component_path) is False:
                os.mkdir(component_path)
                if os.path.exists(os.path.join(component_path, "__init__.py")) is False:
                        file = open(os.path.join(component_path, "__init__.py"), 'w')





def make_compontent(code_path,fun_name):

        code_path = os.path.join(code_path,"./")

        tests_root = os.path.abspath(os.path.dirname(__file__))

        component_path = os.path.join(tests_root,"component")
        code_src_path = os.path.join(tests_root, "component/src")

        mkdir_add_init(component_path)
        mkdir_add_init(code_src_path)

        subprocess.call(f"cp -r {code_path}  {code_src_path}",shell=True)



        python_path = os.path.join(code_src_path,"hash")

        import importlib

        hash  = importlib.import_module("component.src.hash")

        args = hash.get_input()




        #sys.path.append(component_path)
        #from  component.src.hash import get_argumentParser





code_path = "/Volumes/MachineLearning/gitlab/AutoProduceComponent/components/hash/src"

make_compontent(code_path,"")



