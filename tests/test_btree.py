__author__ = "Jeremy Nelson"
__license__ = "GPL Affero"

import datetime
import hashlib
import os
import sys
import tempfile
import unittest
import uuid

import click
import rdflib
from bplustree import BPlusTree, StrSerializer

PROJECT_DIR = os.path.abspath(os.curdir)
sys.path.append(PROJECT_DIR)

import cache.btree as btree

class Test_add_entry(unittest.TestCase):
    
    def setUp(self):
        self.entity = rdflib.URIRef("http://bibcat.org/test-entity")
        self.sha1_key = hashlib.sha1(str(self.entity).encode()).hexdigest()
        self.filepath = os.path.join(PROJECT_DIR, "test-tree.db")
        self.entity_tree = BPlusTree(
            self.filepath,
            serializer=StrSerializer(),
            key_size=40)

    def test_args(self):
        test_sha1 = btree.add_entity(self.entity_tree,
            self.entity)
        self.assertEqual(test_sha1, self.sha1_key)
        self.assertIs(len(self.entity_tree), 1)

    def test_value(self):
        test_sha1 = btree.add_entity(self.entity_tree,
            self.entity)
        self.assertEqual(str(self.entity).encode(),
            self.entity_tree.get(test_sha1))

    def test_authority_url(self):
        authority_url='http://example.com/'
        bnode = rdflib.BNode()
        skolemized_bnode_sha1 = btree.add_entity(
            self.entity_tree,
            bnode,
            authority_url=authority_url)

        skolemized_bnode = self.entity_tree.get(skolemized_bnode_sha1)
        self.assertTrue(
            skolemized_bnode.decode().startswith(authority_url))

    def test_invalid_authority_url(self):
        bnode = rdflib.BNode()
        self.assertRaises(TypeError,
            btree.add_entity,
            self.entity_tree,
            bnode,
            authority_url=1)
 
    def test_duplicate_insert(self):
        test_sha1 = btree.add_entity(self.entity_tree,
            self.entity)
        initial_size = len(self.entity_tree)
        second_sha1 = btree.add_entity(self.entity_tree,
            self.entity)
        self.assertEqual(test_sha1, second_sha1)
        # On disk btree did not change
        self.assertEqual(
            initial_size,
            len(self.entity_tree))

        
    def test_missing_args(self):
        # Missing BTree
        self.assertRaises(TypeError, 
            btree.add_entity, 
            None,
            rdflib.BNode())
       
        # Missing RDF term
        self.assertRaises(TypeError, 
            btree.add_entity, 
            self.entity_tree,
            None)

    def test_missing_authority_url(self):
        bnode = rdflib.BNode()
        skolemized_bnode_sha1 = btree.add_entity(
            self.entity_tree,
            bnode)
        skolemized_bnode = self.entity_tree.get(skolemized_bnode_sha1)
        self.assertTrue(
            skolemized_bnode.decode().startswith('http://rdlib.net'))
        


    def test_truncate_10(self):
        entity_two = rdflib.URIRef("http://bibcat.org/test-entity-tow")
        two_sha1 = btree.add_entity(self.entity_tree, entity_two, 10)
        self.assertEqual(self.entity_tree.get(two_sha1[0:10]),
                         str(two_sha1).encode())

    def test_wrong_type_args(self):

        # Wrong Type for entity_tree
        self.assertRaises(TypeError,
            btree.add_entity,
            dict,
            self.entity)

        # Wrong Type for 
        self.assertRaises(TypeError,
            btree.add_entity,
            self.entity_tree,
            dict)

       
    def tearDown(self):
        self.entity_tree.close()
        os.remove(self.filepath)

class Test_add_patterns(unittest.TestCase):

    def setUp(self):
        self.filepath = os.path.join(PROJECT_DIR, "test-tree.db")
        self.triples_tree = BPlusTree(
            self.filepath,
            order=25,
            serializer=StrSerializer(),
            key_size=124)
        self.graph = rdflib.Graph()
        self.item_iri = rdflib.URIRef(
            "http://example.org/{}".format(uuid.uuid1()))
        self.item_iri_sha1 = hashlib.sha1(
            str(self.item_iri).encode()).hexdigest()
        self.graph.add((self.item_iri, 
            rdflib.RDF.type, 
            rdflib.RDFS.Resource))
        self.graph.add((self.item_iri, 
            rdflib.RDFS.label, 
            rdflib.Literal("Example Item", lang="en")))
        

    def test_wrong_sha1_types(self):

        self.assertRaises(TypeError,
            self.triples_tree,
            btree.add_patterns,
            1,
            str(),
            str())

        self.assertRaises(TypeError,
            self.triples_tree,
            btree.add_patterns,
            str(),
            1,
            str())

        self.assertRaises(TypeError,
            self.triples_tree,
            btree.add_patterns,
            str(),
            str(),
            1)

    def test_simple_ingestion(self):
        for s, p, o in self.graph:
            self.assertTrue(
                btree.add_patterns(self.triples_tree,
                    btree.add_entity(self.triples_tree, s),
                    btree.add_entity(self.triples_tree, p),
                    btree.add_entity(self.triples_tree, o))
            )
        self.assertEqual(
            self.triples_tree.get(self.item_iri_sha1).decode(),
            str(self.item_iri))
        

    def tearDown(self):
        self.triples_tree.close()
        os.remove(self.filepath)

class TestTriplePatternSelectorMatchAll(unittest.TestCase):

    def setUp(self):
        self.filepath = os.path.join(PROJECT_DIR, "test-match-all-tree.db")

        
    def test_empty_metadata(self):
        selector = btree.TriplePatternSelector(
            db_tree_path=self.filepath)
        self.assertEqual(
            int(selector.metadata.get("object")),
            0)
        del selector

    def test_populated_metadata(self):
        test_entity = rdflib.URIRef(
            "http://example.org/{}".format(uuid.uuid1()))
        test_sha1 = hashlib.sha1(str(test_entity).encode()).hexdigest()
        selector = btree.TriplePatternSelector(
            db_tree_path=self.filepath)
        btree.add_patterns(selector.db_tree,
            test_sha1,
            hashlib.sha1(str(rdflib.RDF.type).encode()).hexdigest(),
            hashlib.sha1(str(rdflib.RDFS.Resource).encode()).hexdigest()
        )
        selector.__process__()
        self.assertEqual(
            int(selector.metadata.get("object")),
            1)
        del selector
       
        

    def tearDown(self):
        os.remove(self.filepath)


if __name__ == '__main__':
    unittest.main()
