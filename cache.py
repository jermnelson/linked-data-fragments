__author__ = "Jeremy Nelson, Aaron Coburn, Mark Matienzo"

import asyncio
import aioredis

@asyncio.coroutine
def exists(key):
    redis = yield from aioredis.create_redis(
        ('localhost', 6379), loop=loop)
    result = yield from redis.exists(key)
    redis.close()
    return result

@asyncio.coroutine
def set_subject(key, value):
    redis = yield from aioredis.create_redis(
        ('localhost', 6379), loop=loop)
    # Sets a simple string key-value
    yield from redis.set(key, value)
    # Sets an expire for 1 week for volatile-lru, this value
    # should be set in a config 
    yield from redis.expire(key, 604800)
    redis.close()

