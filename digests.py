"""Module contains commonly used SHA1 digests in the Linked Data Fragments
Data Store and helper functions"""

__author__ = "Jeremy Nelson"

import hashlib
import rdflib

def get_sha1_digest(value):
    """Function takes a unicode string and returns it's sha1 digest

    Args:
       value -- Unicode string

    Returns:
       sha1 of value
    """
    return hashlib.sha1(value.encode()).hexdigest()

OWL = {
    "http://www.w3.org/2002/07/owl#sameAs": "7bffe77e6f9af628763e215707119bc2dbc9b927"
}

RDF = {
    "http://www.w3.org/1999/02/22-rdf-syntax-ns#type": "3c197cb1f6842dc41aa48dc8b9032284bcf39a27"
}

RDFS = {
    "http://www.w3.org/2000/01/rdf-schema#label": "9ac796fdb3c1f82ad26a447b600262114a19983b"
}

