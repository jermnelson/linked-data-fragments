__author__ = "Jeremy Nelson"

import json
import redis

# SPARQL statements
TRIPLE_SPARQL = """SELECT DISTINCT *
WHERE {{{{
  {} {} {} . 
}}}}"""

class Cache(object):

    def __init__(self, **kwargs):
        self.lua_scripts = dict()
        self.cache = kwargs.get('cache', None) 
        if not self.cache:
            self.cache = redis.StrictRedis()
        lua_location = kargs.get('lua_location', None) 
        if not lua_location:
            base_dir = os.path.dirname(os.path.abspath(__name__))[1]
            lua_location = os.path.join(base_dir, "redis")
        for name in ["add_get_hash", 
                     "add_get_triple",
                     "triple_pattern_search"]:
            filepath = os.path.join(
                lua_location, "{}.lua".format(name))
            with open(filepath) as fo:
                lua_script = fo.read()
            sha1 = self.cache.script_load(lua_script)
            setattr(self, name, sha1)

    def triple_search(self, subject, predicate, object_):
        triple_str = self.cache.evalsha(
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
        

