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
    if subject_key == "*":
        predicate_subjects_pattern = "{}:subj-obj".format(
            predicate_key)
        for row in pickle.loads(RDF_TREE.get(predicate_subjects_pattern, [])):
            subject_sha1, object_sha1 = row.split(":")
            results.append({"s": RDF_TREE.get(subject_sha1),
                            "p": RDF_TREE.get(predicate_key),
                            "o": RDF_TREE.get(object_sha1)})

        object_subjects_pattern = "{}:subj-pred".format(
            object_key)
        for row in pickle.loads(RDF_TREE.get(object_subjects_pattern, [])):
            subject_sha1, predicate_sha1 = row.split(":")
            results.append({"s": RDF_TREE.get(subject_sha1),
                            "p": RDF_TREE.get(predicate_sha1),
                            "o": RDF_TREE.get(object_key)})
        results.extend(pickle.loads(RDF_TREE.get(object_subjects_pattern, [])))
    elif predicate_key == "*":
        pass
    elif object_key == "*":
        pass
    else:
        results.append({"s": RDF_TREE.get(subject_key),
                        "p": RDF_TREE.get(predicate_key),
                        "o": RDF_TREE.get(object_key)})
    return results


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
    def __add_pickle_list__(key, value):
        """Helper function"""
        if key in btree:
            pickle_list = pickle.loads(btree.get(key))
            pickle_list.append(value)
        else:
            pickle_list = [value,]
        pickle_list = list(set(pickle_list))
        btree[key] = pickle.dumps(pickle_list)
    key = "{}:{}:{}".format(subject_sha1, predicate_sha1, object_sha1)
    if not key in btree:
        btree.insert(key, b"1")
    __add_pickle_list__("{}:pred-obj".format(subject_sha1),
                        "{}:{}".format(predicate_sha1, object_sha1))
    __add_pickle_list__("{}:subj-obj".format(predicate_sha1),
                        "{}:{}".format(subject_sha1,object_sha1))
    __add_pickle_list__("{}:subj-pred".format(object_sha1),
                        "{}:{}".format(subject_sha1,
                                    predicate_sha1))
    return True
