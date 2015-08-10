__author__ = "Jeremy Nelson"

import falcon
import os
import rdflib

try:
    from config import config
except ImportError:
    config = {"debug": True,
              "cache": "Cache",
              "redis": {"host": "localhost",
		        "port": 6379,
		        "ttl": 604800},
              "rest_api": {"host": "localhost",
                           "port": 18150},
              # Blazegraph SPARQL Endpoint
              "triplestore": {"host": "localhost",
                              "port": 8080,
                              "path": "bigdata"},
              
    }

if config['cache'].startswith("TwemproxyCache"):
    from cache.twemproxy import TwemproxyCache
    CACHE = TwemproxyCache(**config)
elif config['cache'].startswith("ClusterCache"):
    from cache.cluster import ClusterCache
    CACHE = ClusterCache(**config)
else:
    from cache import Cache
    CACHE = Cache(**config)

rest = falcon.API()


# Hooks
def triple_key(req, resp, params):
    subj = params.get('s', None)
    pred = params.get('p', None)
    obj = params.get('o', None)
    triple_str, resp.body = None, None
    if subj and pred and obj:
        triple_str = CACHE.evalsha(
            LUA_SCRIPTS["add_get_triple"],
            4,
            LUA_SCRIPTS["add_get_hash"],
            subj,
            pred,
            obj)
        if triple_str and CACHE.exists(triple_str):
            resp.body = json.dumps(CACHE.get(triple_str))
        elif triple_str:
            resp.body = json.dumps(
                {"missing-triple-key": triple_str}
            )
        else:
            raise falcon.HTTPNotFound()
 
class Triple:

    def __init__(self, **kwargs):
        self.triplestore_url = kwargs.get("triplestore_url", None)
        if not self.triplestore_url:
            self.triplestore_url = "{}:{}/{}".format(
                config.get('triplestore').get('host'),
                config.get('triplestore').get('port'),
                config.get('triplestore').get('path'))

    @falcon.before(triple_key)
    def on_get(self, req, resp):
        if not resp.body:
            # Should search SPARQL endpoint and add to cache
            # if found
            result = requests.post(self.triplestore_url,
                data={"query": TRIPLE_SPARQL.format(req.args.get('s'),
                                                    req.args.get('p'),
                                                    req.args.get('o')),
                      "format": "json"})
            if result.status_code < 399:
                bindings = result.get('results').get('bindings')
                if len(bindings) > 0:
                    for binding in bindings:
                        print(binding)
                        
            else:        
                raise falcon.HTTPNotFound()
        resp.status = falcon.HTTP_200        
            
   
#       raise falcon.HTTPInternalServerError(
#            "Failed to retrieve triple key",
#            "Subject={} Predicate={} Object={}".format(req.args.get('s'), 
#                                                       req.args.get('p'),
#                                                       req.args.get('o')))
         
    @falcon.before(triple_key)          
    def on_post(self, req, resp):
        if resp.body:
            print(resp.body)
        else:
            raise falcon.HTTPInvalidRequest()    
        resp.status = falcon.HTTP_201     

triple = Triple()
rest.add_route("/", triple)
#rest.add_route('/{subj}/{pred}/{obj}', triple)
#rest.add_route('/{subj}:{pred}:{obj}', triple)

if __name__ == '__main__':
    print(LUA_SCRIPTS)
    if config.get('debug'):
        from werkzeug.serving import run_simple
        run_simple(
            config.get('rest_api').get('host'),
            config.get('rest_api').get('port'),
            rest,
            use_reloader=True)
    else:
        print("Production mode not support")
    
