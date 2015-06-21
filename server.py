__author__ = "Jeremy Nelson, Aaron Coburn, Mark Matienzo"

import asyncio
import cache
import rdflib
import shlex




@asyncio.coroutine
def check_add(resource):
    """Coroutine attempts to retrieve the  from cache,
    if not present in cache, attempts to retrieve the sha1
    hashed value from the cache, otherwise adds the subject
    to the cache with the serialized value. 

    Args:
        value -- Subject value
    """
    rHash = cache.add_get_key(resouce)
    return rHash 

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
    loop = asyncio.get_event_loop()
    coro = loop.create_server(LinkedDataFragmentsServer, '0.0.0.0', 9000)
    server = loop.run_until_complete(coro)
    print("Linked-data Fragments Server running on {}".format(
        server.sockets[0].getsockname()))
    try:
        loop.run_forever()
    finally:
        server.close()
        loop.close()

