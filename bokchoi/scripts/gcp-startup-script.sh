#!/bin/bash

apt-get install unzip

gsutil cp gs://${BUCKET_NAME}/${PACKAGE_NAME} .
unzip ${PACKAGE_NAME}
python ${ENTRYPOINT}
