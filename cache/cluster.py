__author__ = "Jeremy Nelson"

import hashlib
from rediscluster import StrictRedisCluster
from . import Cache

class ClusterCache(Cache):

    def __init__(self, **kwargs):
        startup_nodes = kwargs.get("startup_nodes")
        if not startup_nodes:
            startup_nodes = [{"port": 30001, "host": "0.0.0.0"},
                             {"port": 30002, "host": "0.0.0.0"}
        self.cache = StrictRedisCluster(startup_nodes)


    def __get_sha1__(self, value):
        return hashlib.sha1(value).hexdigest() 

    def triple_search(self, subject, predicate, object_):
        triple_str = "{}:{}:{}".format(
            self.__get_sha1__(subject), 
            self.__get_sha1__(predicate),
            self.__get_sha1__(
