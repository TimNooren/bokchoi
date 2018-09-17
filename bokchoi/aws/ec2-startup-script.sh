#!/bin/bash

export REGION={region}
export BOKCHOI_PROJECT_ID={project_id}

# Install aws-cli
sudo curl "https://s3.amazonaws.com/aws-cli/awscli-bundle.zip" -o "awscli-bundle.zip"
python3 -c "import zipfile; zf = zipfile.ZipFile('/awscli-bundle.zip'); zf.extractall('/');"
sudo chmod u+x /awscli-bundle/install
python3 /awscli-bundle/install -i /usr/local/aws -b /usr/local/bin/aws

# Download project zip
aws s3 cp s3://{bucket}/{package} /tmp/
python3 -c "import zipfile; zf = zipfile.ZipFile('/tmp/{package}'); zf.extractall('/tmp/');"

# Make cloudwatch logger executable and fix line endings
sudo chmod u+x /tmp/cloudwatch_logger.py
sed -i $'s/\\r$//' /tmp/cloudwatch_logger.py    # Convert Windows line endings to unix

echo "Downloaded and unpacked project zip" | /tmp/cloudwatch_logger.py bokchoi

# Install pip3 and install requirements.txt from project zip if included
curl -sS https://bootstrap.pypa.io/get-pip.py | sudo python3
[ -f /tmp/requirements.txt ] && pip3 install -r /tmp/requirements.txt && pip3 install boto3

echo "Installed requirements" | /tmp/cloudwatch_logger.py bokchoi

if [ "{notebook}" = "True" ]
then
    #Add public key
    echo "ssh-rsa {public_key}" >> /home/ubuntu/.ssh/authorized_keys
    #Install Jupyter
    pip3 install jupyter
    pip3 install jupyterlab
    jupyter serverextension enable --py jupyterlab --sys-prefix
    pip3 install tornado==4.5.2
    echo "c.NotebookApp.token = u''" >> ~/.jupyter/jupyter_notebook_config.py
    jupyter lab --no-browser --allow-root --ip=0.0.0.0 --port=8888 --NotebookApp.token=
else
    # Run app
    cd /tmp

    echo "Running app" | /tmp/cloudwatch_logger.py bokchoi

    python3 -u {entrypoint} | ./cloudwatch_logger.py app
    aws s3 cp /var/log/cloud-init-output.log s3://{bucket}/cloud-init-output.log

    echo "Finished running app" | /tmp/cloudwatch_logger.py bokchoi

    if [ "{shutdown}" = "True" ]
    then
        echo "Shutting down..." | /tmp/cloudwatch_logger.py bokchoi
        echo "log-termination" | /tmp/cloudwatch_logger.py bokchoi
        shutdown -h now
    fi

    echo "log-termination" | /tmp/cloudwatch_logger.py end

fi