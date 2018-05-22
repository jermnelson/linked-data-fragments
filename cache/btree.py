"""Module overloads Redis digest/cache functions for on-disk BTree"""
__author__ = "Jeremy Nelson"

import asyncio
import hashlib
import pickle
import sys

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
    
    else:
        results.append({"s": RDF_TREE.get(subject_key),
                        "p": RDF_TREE.get(predicate_key),
                        "o": RDF_TREE.get(object_key)})
    return results

@asyncio.coroutine
def get_value(digest):
    if RDF_TREE:
        return RDF_TREE.get(digest)

def add_patterns(btree,
    subject_sha1,
    predicate_sha1,
    object_sha1):
    """Function takes a BTree and subject, predicate, object sha1 and
    adds as a node to a btree.

    Args:
        btree(BPlusTree): RDF Graph Linked Fragments BTree
        subject_sha1(str): SHA1 of subject
        predicate_sha1(str): SHA1 of predicate
        object_sha1(str): SHA1 of object
    
    Returns:
        boolean
    """
    triple_key = "{}>{}>{}".format(subject_sha1,
                                   predicate_sha1,
                                   object_sha1)
    if not triple_key in btree:
        btree.insert(triple_key, b'1')
    predicate_path_key = "{}>{}.{}".format(
        predicate_sha1,
        object_sha1,
        subject_sha1)
    if not predicate_path_key in btree:
        btree.insert(predicate_path_key, b'1')
    object_path_key = "{}.{}>{}".format(
        object_sha1,
        subject_sha1,
        predicate_sha1)
    if not object_path_key in btree:
        btree.insert(object_path_key, b'1')
    return True
 
