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

def remove_expired(**kwargs):
    datastore = kwargs.get("datastore", redis.StrictRedis())
    strategy= kwargs.get("strategy", "string")
    database = kwargs.get('db', 0)
    if strategy.startswith('string'):
        return
    expired_key_notification = "__keyevent@{}__:expired"
    expired_pubsub = datastore.pubsub()
    expired_pubsub.subscribe(expired_key_notification)
    for item in expired_pubsub.listen():
        sha1 = item.get("data")
        transaction = datastore.pipeline(transaction=True)
        remove_subject(sha1, transaction, datastore)
        remove_predicate(sha1, transaction, datastore)
        remove_object(sha1, transaction, datastore)
        transaction.execute()
    
def remove_object(digest, transaction, datastore=redis.StrictRedis()):
    object_key = "{}:subj-pred".format(digest)
    if not datastore.exists(object_key):
        return
    for row in datastore.smembers(object_key):
        subject_digest, predicate_digest = row.split(":")
        subj_pred_obj = "{}:pred-obj".format(subject_digest)
        if datastore.exists(subj_pred_obj):
            transaction.srem(
                subj_pred_obj,
                "{}:{}".format(predicate_digest, digest))
        pred_subj_obj = "{}:subj-obj".format(predicate_digest)
        if datastore.exists(pred_subj_obj):
            transaction.srem(
                pred_subj_obj,
                "{}:{}".format(subject_digest, digest))
    transaction.delete(object_key)


def remove_predicate(digest, transaction, datastore=redis.StrictRedis()):
    predicate_key = "{}:subj-obj".format(digest)
    if not datastore.exists(predicate_key):
        return
    for row in datastore.smembers(member_key):
        subject_digest, object_digest = row.split(":")
        subj_pred_obj = "{}:pred-obj".format(subject_digest)
        if datastore.exists(subj_pred_obj):
            transaction.srem(
                subj_pred_obj,
                "{}:{}".format(digest, object_digest))
        obj_subj_pred = "{}:subj-pred".format(object_digest)
        if datastore.exists(obj_subj_pred):
            transaction.srem(
                obj_subj_pred,
                "{}:{}".format(subject_digest, digest))
    transaction.delete(predicate_key)
                

def remove_subject(digest, transaction, datastore=redis.StrictRedis()):
    subject_key = "{}:pred-obj".format(digest)
    if not datastore.exists(subject_key):
        return
    for row in datastore.smembers(subject_key):
        predicate, object_ = row.split(":")
        pred_subj_obj = "{}:subj-obj".format(predicate)
        if datastore.exists(pred_subj_obj):
            transaction.srem(pred_subj_obj,
                             "{}:{}".format(digest, object_))
        obj_subj_pred = "{}:subj-pred".format(object_)
        if datastore.exists(obj_subj_pred):
            transaction.srem(
                obj_subj_pred,
                "{}:{}".format(digest, predicate))
    transaction.delete(subject_key)

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
            lua_location = os.path.join(base_dir, "lib")
        for name in ["get_triple", 
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
        

