__author__ = "Jeremy Nelson"

import redis
import socket
from . import Cache

class TwemproxyCache(Cache):

    def __init__(self):
        pass

    def triple_search(self, 
            subject=None, 
            predicate=None, 
            object_=None):
        pass
