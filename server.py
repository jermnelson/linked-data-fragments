__author__ = "Jeremy Nelson, Aaron Coburn, Mark Matienzo"

import argparse
import asyncio
from aiohttp import web
import cache.aio as cache
import rdflib
import shlex

try:
    from config import config
except ImportError:
    config = {"debug": True,
              "cache": "Cache",
              "host": "0.0.0.0",
              "port": 7000,
              "redis": {"host": "localhost",
		        "port": 6379,
		        "ttl": 604800},
             # Blazegraph SPARQL Endpoint
              "triplestore": {"host": "localhost",
                              "port": 8080,
                              "path": "bigdata"},
             
              
    }


@asyncio.coroutine
def check_add(resource):
    """Coroutine attempts to retrieve an URL or Literal
    value from cache,
    if not present in cache, attempts to retrieve the sha1
    hashed value from the cache, otherwise adds the subject
    to the cache with the serialized value. 

    Args:
        value -- Subject value
    """
    rHash = cache.add_get_key(resouce)
    return rHash 

@asyncio.coroutine
def handle_triple(request):
    if request.method.startswith('POST'):
        data = request.POST
    elif request.method.startswith('GET'):
        data = request.GET
    else:
        data = {}
    subject_key = yield from cache.get_digest(data.get('s'))
    print("Data={} Subject key={}".format(data.get('s'), subject_key))
    predicate_key = yield from cache.get_digest(data.get('p'))
    object_key = yield from cache.get_digest(data.get('o'))
    result = yield from cache.get_triple(subject_key, predicate_key, object_key)
    print(result)

@asyncio.coroutine
def init_http_server(loop):
    app = web.Application(loop=loop)
    app.router.add_route('GET', '/', handle_triple)
    server = yield from loop.create_server(app.make_handler(),
                                           config.get('host'), 
                                           config.get('port'))
    if config.get('debug'):
        print("Running HTTP Server at {} {}".format(config.get('host'), 
                                                    config.get('port')))
    return server
 
                                      
@asyncio.coroutine
def init_socket_server(loop):
    server = yield from loop.create_server(LinkedDataFragmentsServer, 
                                           config.get('port'), 
                                           config.get(7000))
    if config.get('debug'):
        print("Running Socket Server at {} {}".format(config.get('port'), 
                                                      config.get(7000)))
    return server

    
@asyncio.coroutine
def sparql_subject(value):
    return "Need SPARQL query"

class LinkedDataFragmentsServer(asyncio.Protocol):

    def connection_made(self, transport):
        """Method 
        Args:
            transport -- ?
        """
        self.transport = transport
        #print("transport type={} methods={}".format(type(self.transport), dir(self.transport)))
      
    def data_received(self, data):
        """Method receives incoming HTTP request data

        Args:
            data -- ?
        """
        print(data, type(data))
        self.transport.write("{}".format("Response").encode())
        self.transport.close()


if __name__ == '__main__':
    parser = argparse.ArgumentParser()
    parser.add_argument(
        'action',
        choices=['socket', 'http'],
        default='http',
        help='Run server as either: socket, http, default is http')
    args = parser.parse_args()
    loop = asyncio.get_event_loop()
    if args.action.lower().startswith('socket'):
        server = loop.run_until_complete(init_socket_server(loop))
    elif args.action.lower().startswith('http'):
        server = loop.run_until_complete(init_http_server(loop)) 
    try:
        loop.run_forever()
    finally:
        server.close()
        loop.close()

