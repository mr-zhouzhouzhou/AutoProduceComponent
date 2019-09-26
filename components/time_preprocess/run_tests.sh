#!/bin/bash -e

cd $(dirname $0)

image_name=timestamp
image_tag=unittest
component_root=$(pwd)

docker run --rm -it -v ${component_root}:/unittest ${image_name}:${image_tag} \
python -m unittest discover --verbose --start-dir /unittest/tests

