"""Module replaces Redis digest/cache functions for on-disk BTree"""
__author__ = "Jeremy Nelson"

import asyncio
import hashlib
import pickle
import re
import sys
import uuid
import urllib.parse
import rdflib
from bplustree import BPlusTree, StrSerializer

OBJECT_RE = re.compile(r"(\w+)\.(\w+)>(\w+)")
PRED_RE = re.compile(r"(\w+)>(\w+).(\w+)")
TRIPLE_RE = re.compile(r"(\w+)>(\w+)>(\w+)")


def add_entity(entity_tree: BPlusTree, 
    entity: rdflib.term,
    truncate: int=0,
    authority_url: str=None):
    """
    Function takes a BPlusTree, a RDF node and generates a SHA1
    and stores the binary encoded value to the entity_tree. 
    If truncate, will take the first number of characters and save
    back to the etree.

    Args:
        authority_url(str): Optional, used to skolemize blank nodes
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
    # skolemize any BNodes as per Linked Data Fragments spec
    if isinstance(entity, rdflib.BNode):
        if authority_url:
            entity = entity.skolemize(
                authority=authority_url)
        else:
            entity = entity.skolemize()
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
    start_key = "{}>{}".format(subject_sha1,
                               predicate_sha1)
    # Start key used for look-up based on subject>predicate
    if not start_key in triples_tree:
        triples_tree.insert(start_key, b'1')
    triple_key = "{}>{}".format(start_key,
                                object_sha1)
    if not triple_key in triples_tree:
        triples_tree.insert(triple_key, b'1')
    predicate_path_start = "{}>{}".format(
        predicate_sha1,
        object_sha1)
    # Start key for look-up on batch predicate>object
    if not predicate_path_start:
        triples_tree.insert(predicate_path_key, b'1')
    predicate_path_key = "{}.{}".format(
        predicate_path_start,
        subject_sha1)
    if not predicate_path_key in triples_tree:
        triples_tree.insert(predicate_path_key, b'1')
    object_path_start = "{}.{}".format(object_sha1,
        subject_sha1)
    if not object_path_start in triples_tree:
        triples_tree.insert(object_path_start, b'1')
    object_path_key = "{}>{}".format(
        object_path_start,
        predicate_sha1)
    if not object_path_key in triples_tree:
        triples_tree.insert(object_path_key, b'1')
    return True

class TriplePatternSelector(object):
    """Provides pattern matching for Linked Data Fragments using a BTree
    data file stored on disk"""
    
    def __init__(self, **kwargs):
        self.subject_selector = kwargs.get("subject")
        if not self.subject_selector:
            self.subject_selector = "?subject"
        self.predicate_selector = kwargs.get("predicate") 
        if not self.predicate_selector:
            self.predicate_selector = "?predicate"
        self.object_selector = kwargs.get("object")
        if not self.object_selector:
            self.object_selector = "?object"
        self.limit = kwargs.get("limit", 1000)
        self.db_path = kwargs.get("db_tree_path")
        self.db_tree = None
        if self.db_path:
            self.db_tree = BPlusTree(
                self.db_path,
                serializer=StrSerializer(),
                order=kwargs.get("order", 25),
                key_size=124)
        self.data = []
        # Sets URI for selector 
        self.base_url = kwargs.get('base_url', 'http://localhost:7000')
        self.__process__()

            
    
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
    
    
    def __process__(self):
        subject_key, predicate_key, object_key = None, None, None
        if not self.subject_selector.startswith("?subject"):
            subject_key = hashlib.sha1(
                str(self.subject_selector).encode()).hexdigest()
        if not self.predicate_selector.startswith("?predicate"):
            predicate_key = hashlib.sha1(
                str(self.predicate_selector).encode()).hexdigest()
        if not self.object_selector.startswith("?object"):
            object_key = hashlib.sha1(
                str(self.object_selector).encode()).hexdigest()
        
        # Try to match an exact triple
        if subject_key and predicate_key and object_key:
            match_key = "{}>{}>{}".format(
                subject_key,
                predicate_key,
                object_key)
            match_result = self.db_tree.get(match_key)
            if match_result:
                self.data.append(
                    {"s": self.subject_selector,
                     "p": self.predicate_selector,
                     "o": self.object_selector}
                )
        elif subject_key and predicate_key:
            match_key = "{}>{}".format(subject_key, predicate_key)
            self.__key_matcher__(match_key, TRIPLE_RE)
        elif subject_key:
            self.__key_matcher__(subject_key, TRIPLE_RE)
        else:
            # Returns all subjects matching a predicate>object
            # pattern
            if predicate_key and object_key:
                match_key = "{}>{}".format(
                    predicate_key,
                    object_key)
                self.__key_matcher__(match_key, PRED_RE)
            elif predicate_key is not None:
                self.__key_matcher__(object_key, PRED_RE)
            elif object_key is not None:
                self.__key_matcher__(object_key, OBJECT_RE)
            else:         
                self.__all_matcher__()
    
    def __del__(self):
        if hasattr(self, "temp_file"):
            self.temp_file.close()
        if self.db_tree is not None:
            self.db_tree.close()

    def get(self, digest):
        if digest in self.db_tree:
            return self.db_tree.get(digest).decode()

    def __all_matcher__(self):
        if len(self.data) >= self.limit:
            return
        # Matches all triples
        for row in self.db_tree:
            match_triples = TRIPLE_RE.match(row)
            if not match_triples:
                continue
            groups = match_triples.groups()
            self.data.append({
                "s": self.get(groups[0]),
                "p": self.get(groups[1]),
                "o": self.get(groups[2])
             })

    def __key_matcher__(self, 
            entity_key: str, 
            match_re):
        """Base function used in other functions for matching

        Args:
            entity_key(str): SHA1 of entity
            match_re(re): Complied regular expression
        """
        if len(self.data) >= self.limit:
            return
        for row in self.db_tree[entity_key:]:
            if not row.startswith(entity_key):
                break
            match_obj = match_re.match(row)
            if not match_obj:
                continue
            groups = match_obj.groups()
            self.data.append({
                "s": self.get(groups[1]),
                "p": self.get(groups[2]),
                "o": self.object_selector
             })
