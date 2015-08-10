__author__ = "Jeremy Nelson, Aaron Coburn, Mark Matienzo"

import asyncio
import aioredis
import hashlib
import os
try:
    import config
except ImportError:
    config = {"redis": {"host": "localhost",
                        "port": 6379,
                        "ttl": 604800
                        }}

LUA_SCRIPTS ={}

@asyncio.coroutine
def get_digest(value):
    """Get digest takes either an URI/URL or a Literal value and 
    calls the SHA1 for the add_get_hash.lua script.

    Args:
       value -- URI/URL or Literal value
    """
    redis = get_redis()
    yield from redis.execsha(LUA_SCRIPTS['add_get_hash.lua'], 
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
    yield from redis.scan(pattern)
    redis.close()

def server_setup():
    base_dir = os.path.dirname(os.path.abspath(__name__))
    redis_dir = os.path.join(base_dir, "redis")
    import redis
    cache = redis.StrictRedis(host=config.get('redis').get('host'),
                              port=config.get('redis').get('port'))
    for name in ["add_get_hash.lua", "triple_pattern_search.lua"]:
        filepath = os.path.join(redis_dir, name)
        with open(filepath) as fo:
            lua_script = fo.read()
        sha1 = cache.script_load(lua_script)
        LUA_SCRIPTS[name] = sha1   

server_setup()
print(LUA_SCRIPTS)
