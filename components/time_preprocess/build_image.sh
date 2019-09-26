#!/bin/bash -e

if [ ! $1 ]; then
    image_tag=default
elif [ $1 == test ]; then
    image_tag=unittest
else
    echo "Usage: build_image.sh [test]"
fi

image_name=timestamp # Specify the image name here
full_image_name=${image_name}:${image_tag}
base_image_tag=v1

cd $(dirname "$0")
docker build --build-arg IMAGE_TAG=${image_tag} --build-arg BASE_IMAGE_TAG=v1 -t "${full_image_name}" .
# docker push "$full_image_name"




