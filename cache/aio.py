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
    sha1 = DATASTORE.script_load(lua_script)
    LUA_SCRIPTS[name] = sha1

@asyncio.coroutine
def get_digest(value):
    """Get digest takes either an URI/URL or a Literal value and 
    calls the SHA1 for the add_get_hash.lua script.

    Args:
       value -- URI/URL or Literal value
    """
    print("Value is {}".format(value))
    if not value:
        return None
    loop = asyncio.get_event_loop()
    redis = yield from aioredis.create_redis(
        (config.get("redis")["host"], 
         config.get("redis")["port"]), loop=loop)
    sha1_digest = yield from redis.connection.execute(
        b'EVALSHA',
        LUA_SCRIPTS['add_get_hash'], 
        1, 
        value,
        config.get("redis").get('ttl'))
    return sha1_digest
    redis.close()
    

@asyncio.coroutine
def get_redis():
    yield from aioredis.create_redis(
        (config.get("redis")["host"], 
         config.get("redis")["port"]), loop=loop)

@asyncio.coroutine
def get_triple(subject_key, predicate_key, object_key):
    loop = asyncio.get_event_loop()
    redis_instance = yield from aioredis.create_redis(
        (config.get("redis")["host"], 
         config.get("redis")["port"]), loop=loop)
    pattern = str()
    for key in [subject_key, predicate_key, object_key]:
        if key is None:
            pattern += "*:"
        else:
            pattern += "{}:".format(key.decode())
    pattern = pattern[:-1]
    if pattern.startswith("*:*:*"):
        results = yield from redis_instance.scan(0, pattern)
    else:
        results = yield from redis_instance.keys(pattern)
    print(results)
    redis_instance.close()
