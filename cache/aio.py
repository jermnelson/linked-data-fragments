__author__ = "Jeremy Nelson, Aaron Coburn, Mark Matienzo"

import asyncio
import aioredis
import hashlib
import os
import redis
try:
    import config
except ImportError:
    config = {"redis": {"host": "localhost",
                        "port": 6379,
                        "ttl": 604800
                        }}

LUA_SCRIPTS ={}
BASE_DIR = os.path.dirname(os.path.abspath(__name__))
LUA_LOCATION = os.path.join(BASE_DIR, "redis_lib")
DATASTORE = redis.StrictRedis(host=config.get("redis")["host"],
                              port=config.get("redis")["port"])
for name in ["add_get_hash", 
	     "add_get_triple",
	     "triple_pattern_search"]:
    filepath = os.path.join(
	LUA_LOCATION, "{}.lua".format(name))
    with open(filepath) as fo:
        lua_script = fo.read()
    sha1 = self.datastore.script_load(lua_script)
    LUA_SCRIPT[name] = sha1

@asyncio.coroutine
def get_digest(value):
    """Get digest takes either an URI/URL or a Literal value and 
    calls the SHA1 for the add_get_hash.lua script.

    Args:
       value -- URI/URL or Literal value
    """
    #redis = get_redis()
    loop = asyncio.get_event_loop()
    redis = yield from aioredis.create_redis(
        (config.get("redis")["host"], 
         config.get("redis")["port"]), loop=loop)


    yield from redis.connection.execute(
        b'EXECSHA',
        LUA_SCRIPTS['add_get_hash.lua'], 
        1, 
        value,
        config.get("redis").get('ttl'))
    redis.close()
    

@asyncio.coroutine
def get_redis():
    yield from aioredis.create_redis(
        (config.get("redis")["host"], 
         config.get("redis")["port"]), loop=loop)

@asyncio.coroutine
def get_triple(subject_key=None, predicate_key=None, object_key=None):
    redis = get_redis()
    pattern = str()
    for key in [subject_key, predicate_key, object_key]:
        if key is None:
            pattern += "*"
        else:
            pattern += "{}".format(key)
    pattern = pattern[:-1]
    yield from redis.keys(pattern)
    redis.close()
