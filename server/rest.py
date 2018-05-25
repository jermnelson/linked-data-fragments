__author__ = "Jeremy Nelson"


import hashlib
import json
import os

import click
import falcon
import rdflib
import requests
from cache import btree

try:
    from config import config
except ImportError:
    PROJECT_DIR = os.path.abspath(os.curdir)
    DATA_PATH = os.path.join(PROJECT_DIR, "data/default.db")
    config = {
        "debug": True,
        # BTree File
        "DATA_PATH": DATA_PATH, 
        # Blazegraph SPARQL Endpoint
        "TRIPLESTORE_URL": "http://localhost:9999/blazegraph/sparql"
    }

rest = falcon.API()


# Hooks
def triple_key(req, resp, params):
    if len(params) < 1:
        params = req.params
    click.echo(req)
    subj = params.get('s', None)
    pred = params.get('p', None)
    obj = params.get('o', None)
    click.echo("In triple key hook {} {} {}".format(subj, pred, obj))
    triple_patterns = btree.TriplePatternSelector(
        db_tree_path=config["DATA_PATH"],
        subject=subj,
        predicate=pred,
        object=obj)
    output = {"metadata": triple_patterns.metadata,
              "data": triple_patterns.data}
    resp.body = json.dumps(output)
    
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
            self.triplestore_url = config["TRIPLESTORE_URL"]

    @falcon.before(triple_key)
    def on_get(self, req, resp):
        click.echo("IN TRIPLE {}".format(req.params))
        #if not resp.body:
            # Should search SPARQL endpoint and add to cache
            # if found
            #result = requests.post(self.triplestore_url,
            #    data={"query": TRIPLE_SPARQL.format(req.args.get('s'),
            #                                       req.args.get('p'),
            #                                        req.args.get('o')),
            #          "format": "json"})
            #if result.status_code < 399:
            #    bindings = result.get('results').get('bindings')
            #    if len(bindings) > 0:
            #        for binding in bindings:
            #            print(binding)
                        
            #else:        
            #    raise falcon.HTTPNotFound()
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
    
