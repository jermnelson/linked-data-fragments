__author__ = "Jeremy Nelson"


import digests 
import falcon
import hashlib
import json
import os
import rdflib
import requests

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
    if len(params) < 1:
        params = req.params
    subj = params.get('s', None)
    pred = params.get('p', None)
    obj = params.get('o', None)
    triple_str, resp.body = None, None
    if subj and pred and obj:
        triple_str = CACHE.datastore.evalsha(
            CACHE.add_get_triple,
            3,
            subj,
            pred,
            obj)
        if triple_str and CACHE.datastore.exists(triple_str):
            triple_key = triple_str.decode()
            triple_digests = triple_key.split(":")
            resp.body = json.dumps(
                {"key": triple_str.decode(),
                 "subject_sha1": triple_digests[0],
                 "predicate_sha1": triple_digests[1],
                 "object_sha1": triple_digests[2]}
            )
        elif triple_str:
            resp.body = json.dumps(
                {"missing-triple-key": triple_str.decode()}
            )
        else:
            raise falcon.HTTPNotFound()
    # Subject search
    if subj and not pred and not obj:
        pattern = "{}:*:*".format(hashlib.sha1(str(subj).encode()).hexdigest())
        output = {"subject": str(subj),
                  "predicate-objects": []}
        for triple_key in CACHE.datastore.keys(pattern):
            triples = triple_key.decode().split(":")
            output["predicate-objects"].append({"p": CACHE.datastore.get(triples[1]).decode(),
                                                "o": CACHE.datastore.get(triples[-1]).decode()})
        resp.body = json.dumps(output)

def get_triples(pattern):
    cursor = -1
    output = []
    iterations = 0
    while 1:
        iterations += 1
        if cursor == 0:
            break
        elif cursor < 0:
            cursor = 0
        cursor, resources = CACHE.datastore.scan(
	    cursor=cursor,
	    match=pattern,
	    count=1000)
        cursor = int(cursor)
        if len(resources) > 0:
            output.extend(resources)
    return output


def get_types(type_uri):
    """Function takes a type uri and returns all triple keys that
    matches that RDF type for that uri.

    Args:
        type_uri -- URI to search for

    Returns:
        A list of all triples that match the RDF type of the
        type_uri 
    """
    pattern = "*:{}:{}".format(digests.RDF.get(str(rdflib.RDF.type)),
                               digests.get_sha1_digest(type_uri))
    return get_triples(pattern)


def get_graph(pattern):
    graph = rdflib.Graph()
    transaction = CACHE.datastore.pipeline(transaction=True)
    for key in get_triples(pattern):
        transaction.get(key)
    json_triples = transaction.execute()
        

def get_subject_graph(subject):
    """Function takes a subject URI and scans through cache for all
    triples matching the subject

    Args:
        subject -- subject URI
 
    Returns:
        rdflib.Graph made up all triples
    """
    
    pattern = "{}:*:*".format(digests.get_sha1_digest(subject))
    transaction = CACHE.datastore.pipeline
    for key in get_triples(pattern): 
        pass 
 
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
            if 'missing-triple-key' in resp.body:
                print(resp.body)
        else:
            raise falcon.HTTPInternalServerError("Error with server", "Could not set triple")    
        resp.status = falcon.HTTP_201     

triple = Triple()
rest.add_route("/", triple)
#rest.add_route('/{subj}/{pred}/{obj}', triple)
#rest.add_route('/{subj}:{pred}:{obj}', triple)

if __name__ == '__main__':
    if config.get('debug'):
        from werkzeug.serving import run_simple
        run_simple(
            config.get('rest_api').get('host'),
            config.get('rest_api').get('port'),
            rest,
            use_reloader=True)
    else:
        print("Production mode not support")
    
