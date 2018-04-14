
from bokchoi.config import Config
from bokchoi.aws import EMR, EC2
from bokchoi.utils import Response


class Bokchoi:

    backends = {'EC2': EC2, 'EMR': EMR}

    def __init__(self, name, path):

        self.config = Config(name, path)

        if self.config.loaded:
            self._backend = self._init_backend(self.config.platform)

    def _init_backend(self, platform):
        return self.backends[platform](self.config.name, self.config)

    def init(self, platform):
        """ Initialise new project
        :param platform:            Platform used to run application
        :return:                    Response object
        """
        if self.config.loaded:
            return Response(False, 'Project already initialised')

        self._backend = self._init_backend(platform)

        self.config.write(self._backend.default_config)

        return Response(True, 'Project initialised')

    def deploy(self, *args, **kwargs):
        print('Deploying ' + self.config.name)
        self._backend.deploy(path=self.config.path, *args, **kwargs)

    def undeploy(self, dryrun):
        self._backend.undeploy(dryrun)

    def run(self):
        self._backend.run()

    def stop(self, *args, **kwargs):
        self._backend.stop(*args, **kwargs)

    def connect(self, dryrun, *args, **kwargs):
        self._backend.connect(dryrun, *args, **kwargs)

    def status(self):
        self._backend.status()
