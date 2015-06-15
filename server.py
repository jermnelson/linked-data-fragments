__author__ = "Jeremy Nelson, Aaron Coburn, Mark Matienzo"

import asyncio
import cache
import shlex

@asyncio.coroutine
def get_subject(value):
    """Coroutine attempts to retrieve the subject from cache,
    if not present in cache, attempts to retrieve subject with a
    SPARQL query.

    Args:
        value -- Subject value
    """
    subject = yield from cache.exists(value)
    if subject is None:
        subject = yield from sparql_subject(value)  
        yield from cache.set_subject(value, subject)
    return subject

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

