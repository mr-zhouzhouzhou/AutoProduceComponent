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

from ruamel import yaml


def mkdir_add_init(component_path):

        if  os.path.exists(component_path) is False:
                os.mkdir(component_path)
                if os.path.exists(os.path.join(component_path, "__init__.py")) is False:
                        file = open(os.path.join(component_path, "__init__.py"), 'w')




def mk_dockerfile(file_path):
    full_path = os.path.join(file_path,"dockerfile")
    file = open(full_path, 'w')
    file.writelines("ARG BASE_IMAGE_TAG \n")
    file.writelines("FROM spark_ml:$BASE_IMAGE_TAG \n")
    file.writelines("ARG IMAGE_TAG \n")
    file.writelines("RUN if [ ${IMAGE_TAG} == unittest ]; then pip --disable-pip-version-check --no-cache-dir install kfp; fi \\  \n")
    file.writelines("   && rm -rf /tmp/pip-tmp \n")
    file.writelines("COPY ./src /app \n")



def get_yaml_json(inputs, outputs, image, tag,commond, args):

        return {
	            "name": "Spark Hash Preprocess",
	            "inputs": inputs,
	            "outputs": outputs,
	            "implementation": {
		                "container": {
			                    "image": "{}:{}".format(image,tag),
			                    "command": commond,
			                    "args": args
		                        }
	                    }
                }









def get_config():

    return {"input":["input-path-list",
                     "label-name",
                     "test-size",
                     "out-train-path",
                     "out-test-path",
                     "out-train-path-file",
                     "out-test-path-file"],
            "output":{"out-train-path-file":"out-train-path",
                      "out-test-path-file":"out-test-path"}}




def create_build_image(file_path,image_name):
    full_path = os.path.join(file_path, "build_image.sh")
    file = open(full_path, 'w')
    file.write("#!/bin/bash -e  \n")
    file.write("if [ ! $1 ]; then \n")
    file.write("    image_tag=v2 \n")
    file.write("elif [ $1 == test ]; then \n")
    file.write("    image_tag=unittest \n")
    file.write("else \n")
    file.write("    echo 'Usage: build_image.sh [test]' \n")
    file.write("fi \n")
    file.write(f"image_name={image_name} \n")
    file.write("full_image_name=${image_name}:${image_tag} \n")
    file.write("base_image_tag=v1 \n")
    file.write("cd $(dirname '$0')\n")
    file.write("docker build --build-arg IMAGE_TAG=${image_tag} --build-arg BASE_IMAGE_TAG=v1 -t ${full_image_name} .")




def make_compontent(code_path, pyfile_name):

        code_path = os.path.join(code_path,"./")

        tests_root = os.path.abspath(os.path.dirname(__file__))

        component_path = os.path.join(tests_root,"component")
        code_src_path = os.path.join(tests_root, "component/src")

        mkdir_add_init(component_path)
        mkdir_add_init(code_src_path)

        test_path = os.path.abspath("../components/bind/tests")
        run_test_path = os.path.abspath("../components/bind/run_tests.sh")
        subprocess.call(f"cp -r {test_path}  {component_path}",shell=True)

        subprocess.call(f"cp -r {run_test_path}  {component_path}", shell=True)


        subprocess.call(f"cp -r {code_path}  {code_src_path}", shell=True)



        python_path = os.path.join(code_src_path, pyfile_name)


        #import importlib

        #set_input_out = importlib.import_module("component.src.config").get_config()
        set_input_out = get_config()
        input_list = set_input_out["input"]
        output_list = set_input_out["output"]

        inputs = []
        outputs = []
        args = []
        for item in input_list:
            if item not in output_list.keys():
                inputs.append({"name": item.replace("-", "_"),"type":"String"})
                args.append("--" + item)
                args.append({"inputValue": item.replace("-", "_")})

        for item in output_list:
            outputs.append({"name": output_list[item].replace("-", "_"),"type":"String"})

        for item in output_list:
            # outputs.append({"name":value.replace("-","_")})
            args.append("--" + item)
            args.append({"outputPath": output_list[item].replace("-", "_")})

        commond = ["python", pyfile_name+".py"]

        yaml_json = get_yaml_json(inputs, outputs, "bind", "v2", commond, args)


        yamlpath = os.path.join(component_path, "component.yaml")

        # 写入到yaml文件
        with open(yamlpath, "w", encoding="utf-8") as f:
           yaml.dump(yaml_json, f, Dumper=yaml.RoundTripDumper)

        mk_dockerfile(component_path)

        create_build_image(component_path,"hash")




code_path = os.path.abspath("../components/bind/src")
make_compontent(code_path,"bind_preprocess")



