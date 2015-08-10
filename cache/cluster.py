__author__ = "Jeremy Nelson"

from rediscluster import StrictRedisCluster
from . import Cache

class ClusterCache(Cache):

    def __init__(self, startup_nodes):
        pass


