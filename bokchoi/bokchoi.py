
from bokchoi.config import Config
from bokchoi.aws import EMR, EC2
from bokchoi.gcp import GCP


def requires_config(fn):
    """
    Decorator; adds check whether config was loaded successfully.
    :param fn:                      Function to add check to
    :return:                        Decorated function
    """
    def fn_check_for_config(self, *args, **kwargs):
        if not self.config.loaded:
            return fn.__name__ + ' requires config. Run \'bokchoi init NAME\' to initialise.'
        return fn(self, *args, **kwargs)
    return fn_check_for_config


class Bokchoi:

    backends = {'EC2': EC2, 'EMR': EMR, 'GCP': GCP}

    def __init__(self, path):

        self.config = Config(path)

        try:
            self.config.load()
        except FileNotFoundError:
            print('Config not found')
        else:
            self.backend = self.backends[self.config['Platform']](self.config.name, self.config)

    def init(self, name, platform):
        """ Initialise new project
        :param name:                Name of the project
        :param platform:            Platform used to run application
        :return:                    Response object
        """

        if self.config.loaded:
            return 'Project already initialised. Deploy using \'bokchoi deploy\'.'
        self.config.init(name, platform, self.backends[platform].default_config)

        return 'Project initialised. Deploy using \'bokchoi deploy\'.'

    @requires_config
    def deploy(self, *args, **kwargs):
        print('Deploying: ' + self.config.name)
        return self.backend.deploy(path=self.config.path, *args, **kwargs)

    @requires_config
    def undeploy(self, dryrun):
        print('Undeploying: ' + self.config.name)
        return self.backend.undeploy(dryrun)

    @requires_config
    def run(self):
        print('Running: ' + self.config.name)
        return self.backend.run()

    @requires_config
    def stop(self, *args, **kwargs):
        return self.backend.stop(*args, **kwargs)

    @requires_config
    def connect(self, dryrun, *args, **kwargs):
        self.backend.connect(dryrun, *args, **kwargs)

    @requires_config
    def status(self):
        return self.backend.status()
