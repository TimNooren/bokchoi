# Bokchoi

Bokchoi simplifies running Python batch jobs on AWS spot instances. Bokchoi handles requesting spot instances, deploying your code and ensures the spot requests are cancelled when all jobs are finished.

## Getting Started

### Installing

To install bokchoi:

```
pip install bokchoi
```

### Settings


Say you have a project folder with a single python script:
```
YourProjectFolder
└─ deep_nn.py
```
In your project folder create a settings file named **bokchoi_settings.json**:
```
YourProjectFolder
├─ deep_nn.py
└─ bokchoi_settings.json
```
This file should contain the following:

```json
{
  "<projectname>": {
    "EntryPoint": "deep_nn.main",
    "Region": "us-east-1",
    "Platform": 'EC2'
    "Requirements": [
      "numpy==1.13.0",
      "boto3==1.4.4"
    ],
    "EC2": {
      "InstanceCount": 1,
      "SpotPrice": "0.1",
      "LaunchSpecification": {
        "ImageId": "ami-123456",
        "InstanceType": "c4.large",
        "SubnetId": "subnet-123456"
      }
    }
  }
}
```

### Deploying

Deploying your job to AWS is now as simple as running:
```
bokchoi job_name deploy
```
\
Bokchoi will package your project and upload it to S3. You can then use the following command to run your job:
```
bokchoi job_name run
```
\
This will issue a spot request for the number of spot instances specified in the settings file. Every spot instance will download the packaged project from S3 and run the main function. Once the job is complete the instance will shut down. When all instances are finished the spot request will automatically be cancelled.

### Undeploying

To undeploy your job, removing all resources from your AWS environment:
```
bokchoi job_name undeploy
```
\
This will terminate any spot instances related to your job, cancel all spot requests and remove the packaged project from S3. Any IAM resources, such as policies, roles and instance profiles will also be removed.

## Acknowledgements

Shamelessly inspired by Zappa (https://github.com/Miserlou/Zappa)