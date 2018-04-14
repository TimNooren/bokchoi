
import json
import os


class Config:

    def __init__(self, name, path=''):

        self.path = path
        self.config_path = os.path.join(path, 'bokchoi_settings.json')

        try:
            with open(self.config_path, 'r') as config_file:
                config_json = json.load(config_file)
        except FileNotFoundError:
            if name:
                self._map = {}
            else:
                raise KeyError('No config found and no name specified')
        else:
            if not name and len(config_json.keys()) == 1:
                name = list(config_json.keys())[0]
            else:
                raise KeyError('No name specified and multiple names found in config')

            self._map = config_json[name]
        self.name = name

        self.platform = self._map.get('Platform')
        setattr(self, self.platform.lower(), self._map[self.platform])

        self.loaded = bool(self._map)

    def write(self, _map=None):
        with open(self.config_path, 'w') as _file:
            json.dump({self.name: _map or self._map}, _file, indent=4)

    @property
    def platform(self):
        return self._map.get('Platform')

    @property
    def platform(self):
        return self._map.get('Platform')

    @platform.setter
    def platform(self, platform):
        self._map['Platform'] = platform

    @property
    def app(self):
        return self._map.get('App', '.')

    @app.setter
    def app(self, app):
        self._map['App'] = app

    @property
    def connect(self):
        return self._map.get('Connect')

    @connect.setter
    def connect(self, flag):
        self._map['Connect'] = flag

    @property
    def requirements(self):
        return self._map.get('Requirements')

    @requirements.setter
    def requirements(self, flag):
        self._map['Requirements'] = flag

    @property
    def shutdown(self):
        return self._map.get('Shutdown', True)

    @shutdown.setter
    def shutdown(self, flag):
        self._map['Shutdown'] = flag
