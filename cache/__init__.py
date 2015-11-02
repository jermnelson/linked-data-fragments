__author__ = "Jeremy Nelson"

import json
import hashlib
import os
import redis

# Different strategies for storing triple information in
# Redis data structures; 
def hash_pattern(transaction, 
                 subject_sha1, 
                 predicate_sha1,
                 object_sha1):
    pass

def string_pattern(transaction, 
                   subject_sha1, 
                   predicate_sha1,
                   object_sha1):
    """The string pattern is the simplest to implement
    but slow O(n) performance with KEYS and SCAN"""
    transaction.set("{}:{}:{}".format(
        subject_sha1,
        predicate_sha1,
        object_sha1),
        1)

def set_pattern(transaction,
                subject_sha1, 
                predicate_sha1,
                object_sha1):
    transaction.sadd("{}:pred-obj".format(subject_sha1),
                     "{}:{}".format(predicate_sha1,
                                    object_sha1))
    transaction.sadd("{}:subj-obj".format(predicate_sha1),
                     "{}:{}".format(subject_sha1,
                                    object_sha1))
    transaction.sadd("{}:subj-pred".format(object_sha1),
                     "{}:{}".format(subject_sha1,
                                    predicate_sha1))


def add_triple(datastore, subject, predicate, object_, pattern="string"):
    subject_sha1 = hashlib.sha1(subject.encode()).hexdigest()
    predicate_sha1 = hashlib.sha1(predicate.encode()).hexdigest()
    object_sha1 = hashlib.sha1(object_.encode()).hexdigest()
    transaction = datastore.pipeline(transaction=True)
    transaction.set(subject_sha1, subject)
    transaction.set(predicate_sha1, predicate)
    transaction.set(object_sha1, object_)
    if pattern.startswith("string"):
        strategy = string_pattern
    elif pattern.startswith("hash"):
        strategy = hash_pattern
    elif pattern.startswith("set"):
        strategy = set_pattern
    strategy(transaction,
             subject_sha1,
             predicate_sha1,
             object_sha1)
    transaction.execute()

def remove_expired(datastore, strategy="string"):
    if strategy.startswith('string'):
        return
    expired_pubsub = database.pubsub()

# SPARQL statements
TRIPLE_SPARQL = """SELECT DISTINCT *
WHERE {{{{
  {} {} {} . 
}}}}"""

class Cache(object):

    def __init__(self, **kwargs):
        self.lua_scripts = dict()
        redis_config = kwargs.get('redis', None) 
        if redis_config:
            self.datastore = redis.StrictRedis(
                host=redis_config.get('host'),
                port=redis_config.get('port'))
        else:
            self.datastore = redis.StrictRedis()
        lua_location = kwargs.get('lua_location', None) 
        if not lua_location:
            base_dir = os.path.dirname(os.path.abspath(__name__))
            lua_location = os.path.join(base_dir, "redis_lib")
        for name in ["add_get_hash", 
                     "add_get_triple",
                     "triple_pattern_search"]:
            filepath = os.path.join(
                lua_location, "{}.lua".format(name))
            with open(filepath) as fo:
                lua_script = fo.read()
            sha1 = self.datastore.script_load(lua_script)
            setattr(self, name, sha1)

    def __get_sha1__(self, entity):
        return hashlib.sha1(entity.encode()).hexdigest()

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
        

