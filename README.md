# Bokchoi

Bokchoi simplifies running Python batch jobs on AWS spot instances. Bokchoi handles requesting spot instances, deploying your code and ensures the spot requests are cancelled when all jobs are finished.

## Getting Started

### Installing

Installing bokchoi (still on testpypi only):

```
pip install --extra-index-url https://testpypi.python.org/pypi bokchoi
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
├─ deepest_nn.py
└─ bokchoi_settings.json
```
This file should contain the following:

```json
{
  "job_name": {    # Used to deploy your job
    "EntryPoint": "deep_nn.main",  # function called once spot instance is launched
    "Region": "us-east-1",  # The region you wish to deploy to
    "Requirements": [
      "numpy==1.13.0",
      "boto3==1.4.4"
    ],
    "EC2": {
      "InstanceCount": 1,   # Number of spot instances
      "SpotPrice": "0.1",   # Maximum bid price (dollars)
      "LaunchSpecification": {
        "ImageId": "ami-a9210ebf",  # Currently only Ubuntu 16.04 is supported
        "InstanceType": "c4.large",
        "SubnetId": "subnet-2dr85kr9"   # Subnet to deploy spot instances into
      }
    }
  }
}
```

### Deploying

Deploying your job to AWS is now as simple as running:
```
bokchoi deploy job_name
```
\
Bokchoi will package your project and upload it to S3. You can then use the following command to run your job:
```
bokchoi run job_name
```
\
This will issue a spot request for the number of spot instances specified in the settings file. Every spot instance will download the packaged project from S3 and run the main function. Once the job is complete the instance will shut down. When all instances are finished the spot request will automatically be cancelled.

### Undeploying

To undeploy your job, removing all resources from your AWS environment:
```
bokchoi undeploy job_name
```
\
This will terminate any spot instances related to your job, cancel all spot requests and remove the packaged project from S3. Any IAM resources, such as policies, roles and instance profiles will also be removed.

## Acknowledgements

Shamelessly inspired by Zappa (https://github.com/Miserlou/Zappa)