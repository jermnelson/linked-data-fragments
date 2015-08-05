__author__ = "Jeremy Nelson"

import falcon
import os
import redis
import rdflib

try:
    import config
except ImportError:
    config = {"debug": True,
              "redis": {"host": "localhost",
		        "port": 6379,
		        "ttl": 604800},
              "rest-api": {"host": "localhost",
                           "port": 18150},
              # Blazegraph SPARQL Endpoint
              "triplestore": {"host": "localhost",
                              "port": 8080,
                              "path": "bigdata"},
              
    }

CACHE = None
LUA_SCRIPTS ={}

def server_setup():
    base_dir = os.path.dirname(os.path.abspath(__name__))
    redis_dir = os.path.join(base_dir, "redis")
    CACHE = redis.StrictRedis(host=config.get('redis').get('host'),
                              port=config.get('redis').get('port'))
    for name in ["add_get_hash", 
                 "triple_pattern_search"]:
        filepath = os.path.join(redis_dir, "{}.lua".format(name))
        with open(filepath) as fo:
            lua_script = fo.read()
        sha1 = CACHE.script_load(lua_script)
        LUA_SCRIPTS[name] = sha1   

server_setup()

rest = falcon.API()

class Triple:

    def on_get(self, req, resp, subj, pred, obj):
        result = CACHE.evalsha(
            LUA_SCRIPTS['triple_pattern_search'], 
            subj, 
            pred, 
            obj)    
        resp.body = result
        
    def on_post(self, req, resp, subj, pred, obj):
        triple_sha1 = "{}:{}:{}".format(
            CACHE.evalsha(
                LUA_SCRIPTS["add_get_hash"],
                1,
                subj).decode(),
            CACHE.evalsha(
                LUA_SCRIPTS["add_get_hash"],
                1,
                pred).decode(),
             CACHE.evalsha(
                LUA_SCRIPTS["add_get_hash"],
                1,
                obj).decode())
        resp.header = falcon.HTTP_201
        resp.body = triple_sha1

triple = Triple()
rest.add_route('/{subj}/{pred}/{obj}', triple)
rest.add_route('/{subj}:{pred}:{obj}', triple)

if __name__ == '__main__':
    if config.get('debug'):
        from werkzeug.serving import run_simple
        run_simple(
            config.get('rest-api').get('host'),
            config.get('rest-api').get('port'),
            rest,
            use_reloader=True)
    else:
        print("Production mode not support")
    
