# -*- coding: utf-8 -*-
#
# This file is part of Radicale Server - Calendar Server
# Copyright © 2012 Guillaume Ayoub
#
# This library is free software: you can redistribute it and/or modify
# it under the terms of the GNU General Public License as published by
# the Free Software Foundation, either version 3 of the License, or
# (at your option) any later version.
#
# This library is distributed in the hope that it will be useful,
# but WITHOUT ANY WARRANTY; without even the implied warranty of
# MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
# GNU General Public License for more details.
#
# You should have received a copy of the GNU General Public License
# along with Radicale.  If not, see <http://www.gnu.org/licenses/>.

# Contributed by Mark Adams <mark@markadams.me>

"""
Redis key-value storage backend.

"""

import json
import time
import redis
from contextlib import contextmanager
from datetime import datetime

from .. import config, ical

class Collection(ical.Collection):
    """Collection stored in a redis key-value store."""
    def __init__(self, *args,**kwargs):
        super(Collection, self).__init__(*args,**kwargs)
        self._redis = None

    @property
    def _props_path(self):
        """The path of the resource"""
        return self.path + ".props" 
    
    @classmethod
    def _connect_redis(cls):
        """Creates a connection to the redis server"""
        return redis.StrictRedis(host='localhost')

    def _get_redis(self):
        """Retrieves a redis server connection"""

        # Cache the redis connection for multiple calls
        if self._redis == None:
            self._redis = self._connect_redis()

        return self._redis

    def save(self, text):
        """ Save the message and modified time to redis """
        self._get_redis().set(self.path,text)
        self._get_redis().set(self.path + ':modified', time.time())

    def delete(self):
        """ Delete an entry from redis """
        self._get_redis().delete(self.path)
        self._get_redis().delete(self.path + ':modified')
        self._get_redis().delete(self.props_path)

    @property
    def text(self):
        """ Retrieve the text from redis """
        try:
            return self._get_redis().get(self.path) or ''
        except IOError:
            return ""

    @classmethod
    def children(cls, path):
        raise StopIteration()

    @classmethod
    def is_node(cls, path):
        """ Determine if this path represents a node """
        if cls._connect_redis().exists(path) and path.endswith("/"):
            return True
        else:
            return False 

    @classmethod
    def is_leaf(cls, path):
        """ Determine if this path represents a leaf """
        return cls._connect_redis().exists(path) and not path.endswith(".props")

    @property
    def last_modified(self):
        """ The last modified time of the path """
        modification_time = float(self._get_redis().get(self.path + ':modified') or time.time())
        return time.strftime("%a, %d %b %Y %H:%M:%S +0000", time.localtime(modification_time))

    @property
    @contextmanager
    def props(self):
        # On enter
        if self._props_path: 
            properties = {}
            propstring = self._get_redis().get(self._props_path)
            
            if propstring is not None:
                properties.update(json.loads(propstring))
            
            yield properties
            # On exit
            self._get_redis().set(self._props_path, json.dumps(properties))
          
