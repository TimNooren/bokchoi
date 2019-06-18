#!/bin/bash

if [ "$(whoami)" != "root" ]
then
    sudo su
fi

export DEBIAN_FRONTEND=noninteractive

apt-get update \
    && apt-get -y upgrade \
    && apt-get install -y python3-minimal python3-pip python3-dev unzip \
    && pip3 install --upgrade pip

apt-get update \
    && apt-get -y upgrade \
    && apt-get install -y python3-minimal python3-pip python3-dev unzip \
    && pip3 install --upgrade pip

BUCKET_NAME=$(curl http://metadata/computeMetadata/v1/instance/attributes/bucket_name -H "Metadata-Flavor: Google")
PACKAGE_NAME=$(curl http://metadata/computeMetadata/v1/instance/attributes/package_name -H "Metadata-Flavor: Google")
ENTRYPOINT=$(curl http://metadata/computeMetadata/v1/instance/attributes/entry_point -H "Metadata-Flavor: Google")
INSTANCE_NAME=$(curl http://metadata/computeMetadata/v1/instance/attributes/instance_name -H "Metadata-Flavor: Google")
ZONE=$(curl http://metadata/computeMetadata/v1/instance/attributes/zone -H "Metadata-Flavor: Google")

gsutil cp gs://${BUCKET_NAME}/${PACKAGE_NAME} .
unzip ${PACKAGE_NAME}

python3 -m pip install -r requirements.txt >> logs.txt 2>&1
python3 ${ENTRYPOINT} >> logs.txt 2>&1
gsutil cp logs.txt gs://${BUCKET_NAME}/${PACKAGE_NAME}-logs.txt
gcloud compute instances delete ${INSTANCE_NAME} --zone ${ZONE}
