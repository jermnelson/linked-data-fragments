__author__ = "Jeremy Nelson"
__license__ = "GPL Affero"

import datetime
import hashlib
import os
import sys
import tempfile
import unittest

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

if __name__ == '__main__':
    unittest.main()
