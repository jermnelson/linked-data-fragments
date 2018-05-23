"""Module overloads Redis digest/cache functions for on-disk BTree"""
__author__ = "Jeremy Nelson"

import asyncio
import hashlib
import pickle
import sys
from typing import NewType

import rdflib
from bplustree import BPlusTree, StrSerializer

RDF_TREE = None

@asyncio.coroutine
def get_digest(value):
    """Get digest takes either an URI/URL or a Literal value and 
    calls the SHA1 for the add_get_hash.lua script.

    Args:
       value -- URI/URL or Literal value

    Returns:
        str of value's hex digest
    """
    sha1 = hashlib.sha1(str(value).encode())
    return sha1.hexdigest()

@asyncio.coroutine
def get_triple(subject_key, predicate_key, object_key):
    """Function takes SHA1s of subject, predicate, and object
    and returns a list of triples

    Args:
        subject_key(str): SHA1 of subject
        predicate_key(str): SHA1 of predicate
        object_key(str): SHA1 of object

    Returns:
        list of triples
    """
    results = []
    
        #results.append({"s": RDF_TREE.get(subject_key),
        #                "p": RDF_TREE.get(predicate_key),
        #                "o": RDF_TREE.get(object_key)})
    return results

@asyncio.coroutine
def get_value(digest):
    if RDF_TREE:
        return RDF_TREE.get(digest)

def add_entity(entity_tree: BPlusTree, 
    entity: rdflib.term,
    truncate: int=0):
    """
    Function takes a BPlusTree, a RDF node and generates a SHA1
    and stores the binary encoded value to the entity_tree. 
    If truncate, will take the first number of characters and save
    back to the etree.

    Args:
        entity_tree(BPlusTree): Entity Tree
        entity(rdflib.Node): RDF URIRef, BNode, or Literal
        truncate(int): Number of characters to truncate SHA1 for 
           fragment key.
    """
    if not isinstance(entity_tree, BPlusTree):
        raise TypeError("BPlusTree entity_tree required")
    if not isinstance(entity, rdflib.term.Node):
        raise TypeError(
            "{} Must either be a rdflib URIRef, BNode, or Literal".format(
                entity))
            
    binary_entity = str(entity).encode()
    entity_sha1 = hashlib.sha1(binary_entity).hexdigest()
    if not entity_sha1 in entity_tree:
        entity_tree.insert(entity_sha1, binary_entity)
    if truncate > 0:
        fragment_key = entity_sha1[0:truncate]
        if not fragment_key in entity_tree:
            entity_tree.insert(fragment_key, entity_sha1.encode())
    return entity_sha1

def add_patterns(triples_tree,
    subject_sha1: str,
    predicate_sha1: str,
    object_sha1: str,
    shard_size: int=0):
    """Function takes a BTree and subject, predicate, object sha1 and
    adds as a node to a btree.

    Args:
        triples_tree(BPlusTree): Stores full or abbreviated SHA1 as triples
        subject_sha1(str): SHA1 of subject
        predicate_sha1(str): SHA1 of predicate
        object_sha1(str): SHA1 of object
        shard_size(int): Size of SHA1 fragment
    Returns:
        boolean
    """
    if shard_size > 0:
        subject_sha1 = subject_sha1[0:shard_size]
        predicate_sha1 = predicate_sha1[0:shard_size]
        object_sha1 = object_sha1[0:shard_size]  
    triple_key = "{}>{}>{}".format(subject_sha1,
                                   predicate_sha1,
                                   object_sha1)
    if not triple_key in triples_tree:
        triples_tree.insert(triple_key, b'1')
    predicate_path_key = "{}>{}.{}".format(
        predicate_sha1,
        object_sha1,
        subject_sha1)
    if not predicate_path_key in triples_tree:
        triples_tree.insert(predicate_path_key, b'1')
    object_path_key = "{}.{}>{}".format(
        object_sha1,
        subject_sha1,
        predicate_sha1)
    if not object_path_key in triples_tree:
        triples_tree.insert(object_path_key, b'1')
    return True

class TriplePatternSelector(type):
    
    def __init__(self, **kwargs):
        self.subject_selector = kwargs.get("subject", "?subject")
        self.predicate_selector = kwargs.get("predicate", "?predicate")
        self.object_selector = kwargs.get("object", "?object")
        self.entity_tree = BPlusTree(
            kwargs.get("entity_tree_path"),
            serializer=StrSerializer(),
            key_size=40)
        self.triples_tree = BPlusTree(
            kwargs.get("triples_tree_path"),
            serializer=StrSerializer(),
            key_size=124)
        self.triples_path = kwargs.get("triples_tree_path")
        self.data = []
        # Sets URI for selector 
        self.base_url = kwargs.get('base_url', 'http://localhost:7000')
    
    @property
    def uri(self):
        return urllib.parse.urljoin(
            self.base_url,
            str(uuid.uuid1()))
    
    @property
    def metadata(self):
        return {
            "subject": self.uri,
            "predicate": "void:triples",
            "object": len(self.data)
        }
    
    def __iter__(self):
        return self
    
    def __next__(self):
        if len(self.data) >= 10:
            raise StopIteration
        result = []
        print(self.subject_selector, self.subject_selector.startswith("?subject"))
        if not self.subject_selector.startswith("?subject"):
            subject_key = hashlib.sha1(
                str(self.subject_selector).encode()).hexdigest()
            for key in self.db_tree[subject_key:]:
                if not key.startswith(subject_key):
                    raise StopIteration
                triple_result = triple_pattern.search(key)
                print(key, triple_result)
                if triple_result:
                    triples = triple_result.groups()
                    result.append({"s": self.subject_selector,
                                   "p": self.db_tree.get(triples[1]).decode(),
                                   "o": self.db_tree.get(triples[2]).decode()})
                              
                
                
        #if not self.predicate_selector.startswith("?predicate"):
        #    key, value = yield self.db_tree.items(slice(start=self.predicate_selector))
        #    result['p'] =  value
        #if not self.object_selector.startswith("?object"):
        #key, value = yield self.db_tree.items(slice(start=self.object_selector))
         #   result['o'] = value
        self.data.extend(result)
        return result
    
    def __del__(self):
        if hasattr(self, "temp_file"):
            self.temp_file.close()
        if hasattr(self, "db_path"):
            self.db_tree.close() 
