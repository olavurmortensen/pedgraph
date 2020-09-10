#!/usr/bin/env python3

from neo4j import GraphDatabase
from pedgraph.BuildDB import BuildDB
from pedgraph.ReconstructGenealogy import ReconstructGenealogy
import unittest, logging
from os import environ as env

# Get the URI to
NEO4J_URI = env.get('NEO4J_URI')

# If the NEO4J_URI environment variable is not defined, set it to the default.
if NEO4J_URI is None:
    NEO4J_URI = 'bolt://localhost:7687'

class TestSum(unittest.TestCase):

    def setUp(self):
        logging.info('Startup')
        logging.info('-------')
        builder = BuildDB(NEO4J_URI, 'pedgraph/test/test_data/test_tree.csv')

    def test_recon(self):
        logging.info('Reconstructing genealogy')
        logging.info('------------------------')
        gen = ReconstructGenealogy(NEO4J_URI, probands=['9', '10'])

    def tearDown(self):
        logging.info('-------')
        logging.info('Teardown')

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
