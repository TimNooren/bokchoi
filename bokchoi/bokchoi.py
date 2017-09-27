"""
This script is used to implement the default methods
"""

class Bokchoi(object):
    """Main Instance Type class which cannot be used to deploy applications."""

    def run(self):
        """Run the script given by the user"""
        raise NotImplementedError('You must implement the run() method!')    

    def deploy(self):
        """Create environment, setup policies, and create users"""
        raise NotImplementedError('You must implement the deploy() method!')

    def undeploy(self):
        """Remove all objects created by the deploy function"""
        raise NotImplementedError('You must implement the undeploy() method!')
