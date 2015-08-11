__author__ = "Jeremy Nelson"

import json
import os
import redis

# SPARQL statements
TRIPLE_SPARQL = """SELECT DISTINCT *
WHERE {{{{
  {} {} {} . 
}}}}"""

class Cache(object):

    def __init__(self, **kwargs):
        self.lua_scripts = dict()
        self.datastore = kwargs.get('datastore', None) 
        if not self.datastore:
            self.datastore = redis.StrictRedis()
        lua_location = kwargs.get('lua_location', None) 
        if not lua_location:
            base_dir = os.path.dirname(os.path.abspath(__name__))
            lua_location = os.path.join(base_dir, "redis")
        for name in ["add_get_hash", 
                     "add_get_triple",
                     "triple_pattern_search"]:
            filepath = os.path.join(
                lua_location, "{}.lua".format(name))
            with open(filepath) as fo:
                lua_script = fo.read()
            sha1 = self.datastore.script_load(lua_script)
            setattr(self, name, sha1)

    def triple_search(self, subject=None, predicate=None, object_=None):
        triple_str = self.datastore.evalsha(
            self.add_get_triple,
            3,
            subject,
            predicate,
            object_)
        if triple_str:
            if self.cache.exists(triple_str):
                return json.dumps(self.cache.get(triple_str))
            else:
                return {"result": "Missing Triple Key {}".format(triple_str)}
        

