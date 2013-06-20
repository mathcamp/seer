"""
Library for looking up servers by role

"""
import random
from datetime import datetime, timedelta
import os.path
import yaml
import json
import logging

LOG = logging.getLogger(__name__)

DEFAULT_ROLE_FILE = '/etc/highlight/roles.yaml'

class LiveConfig(dict):
    """
    A config object that mirrors a file on disk and updates if the file changes.

    The config file must be in either yaml or json format. The config values
    may be accessed as either keys or members. For example::

        >>> cfg = LiveConfig('config.yaml')
        >>> cfg['master']
        'master.hl'
        >>> cfg.master
        'master.hl'

    Parameters
    ----------
    filename : str
        Name of the file on disk to load data from
    reload_every : int
        How frequently (in seconds) to read from disk to refresh the data
        (default 60)
    use_timer : bool
        If True, will reload on a tornado timer (default False)

    Notes
    -----
    If you do not enable the `use_timer` parameter, certain calls *can* get
    out-of-date. If you use any method that gets the keys/values directly, such
    as ``items()`` or ``iteritems()``, it will not refresh the values from
    disk.

    TODO: programmatically wrap accessors to call _reload_if_needed

    """
    def __init__(self, filename, reload_every=60, use_timer=False):
        super(LiveConfig, self).__init__()
        self._filename = filename
        self._reload_every = reload_every
        self._use_timer = use_timer
        self._last_load = None
        self._reload()
        if use_timer:
            from tornado import ioloop # pylint: disable=F0401
            callback = ioloop.PeriodicCallback(self._reload, reload_every*1000)
            callback.start()

    def _reload(self):
        """Reload config from disk"""
        self._last_load = datetime.utcnow()
        if not os.path.exists(self._filename):
            LOG.warning("%s not found", self._filename)
            return
        stream = open(self._filename)
        try:
            if self._filename.endswith(".yaml"):
                self.clear()
                self.update(yaml.load(stream))
            elif self._filename.endswith(".json"):
                self.clear()
                self.update(json.load(stream))
            else:
                raise Exception("Unsupported file format! " + self._filename)
        except:
            LOG.error("Bad config format! " + self._filename)

    def _reload_if_needed(self):
        """Reload from disk if our data is old and we're not on a timer"""
        delta = timedelta(seconds=self._reload_every)
        if not self._use_timer and datetime.utcnow() > self._last_load + delta:
            self._reload()

    def __getitem__(self, key):
        self._reload_if_needed()
        return super(LiveConfig, self).__getitem__(key)

    def get(self, key, default=None):
        try:
            return self.__getitem__(key)
        except KeyError:
            return default

    def __delitem__(self, key):
        raise KeyError("LiveConfig is read-only!")

    def setdefault(self, key, val):
        raise KeyError("LiveConfig is read-only!")

    def __getattr__(self, key):
        try:
            return self.__getitem__(key)
        except KeyError:
            raise AttributeError("No attribute %s" % key)


class Seer(object): # pylint: disable=R0924
    """
    Wrapper for looking up servers by role

    Parameters
    ----------
    role_file : str, optional
        Path to the role file to track (default :const:`.DEFAULT_ROLE_FILE`)
    use_timer : bool, optional
        Use a tornado timer to reload the file (default False)

    """
    def __init__(self, role_file=DEFAULT_ROLE_FILE, use_timer=False):
        self._roles = LiveConfig(role_file, reload_every=10,
                                 use_timer=use_timer)

    def __getitem__(self, key):
        try:
            name = random.choice(self._roles[key].keys())
        except IndexError:
            raise KeyError("%s not found" % key)
        data = self._roles[key][name]
        if data:
            data = dict(data)
        else:
            data = {}
        data['name'] = name
        return data
