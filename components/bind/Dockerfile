ARG BASE_IMAGE_TAG
FROM spark_ml:$BASE_IMAGE_TAG
ARG IMAGE_TAG
SHELL ["/bin/bash", "-c"]
RUN if [ ${IMAGE_TAG} == unittest ]; then pip --disable-pip-version-check --no-cache-dir install kfp; fi \
    && rm -rf /tmp/pip-tmp
COPY ./src /app




