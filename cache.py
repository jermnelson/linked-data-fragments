__author__ = "Jeremy Nelson, Aaron Coburn, Mark Matienzo"

import asyncio
import aioredis
import hashlib
try:
    import config
except ImportError:
    config = {"redis": {"host": "localhost",
                        "port": 6379,
                        "ttl": 604800
                        }}
@asyncio.coroutine
def add_get_key(resource_hash, resource_str):
    redis = get_redis()    
    if not redis.exists(resource_hash):
        redis.hset(resource_hash, "source", resource_str)
    ttl = config.get("redis").get("ttl", 604800)
    redis.expire(resource_hash, ttl)
    return resource_hash

@asyncio.coroutine
def exists(key):
    redis=get_redis()
    result = yield from redis.exists(key)
    redis.close()
    return result

@asyncio.coroutine
def get_redis():
    yield from aioredis.create_redis(
        (config.get("redis")["host"], 
         config.get("redis")["port"]), loop=loop)

@asyncio.coroutine
def expand_namepace(resource):
    redis = get_redis()
    namespace_str, value = str(resource).split(":")
    url = yield from redis.hget("namespaces", namespace_str)
    if url:
        namespace = rdflib.Namespace(url)
        yield from getattr(namespace, value)  
    

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

