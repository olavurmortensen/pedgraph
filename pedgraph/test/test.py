#!/usr/bin/env python3

# TODO:
# Write to DB, read from DB, write to CSV, and compare with original CSV.

from neo4j import GraphDatabase
from pedgraph.BuildDB import BuildDB
from pedgraph.WriteCSV import WriteCSV
import unittest, logging
from os import environ as env

# Get the URI to
NEO4J_URI = env.get('NEO4J_URI')

# If the NEO4J_URI environment variable is not defined, set it to the default.
if NEO4J_URI is None:
    NEO4J_URI = 'bolt://localhost:7687'

class TestSum(unittest.TestCase):

    def setUp(self):
        tt = 1
        #builder = BuildDB(, )
        #writer = WriteCSV(, )

    def test_db_connection(self):
        driver = GraphDatabase.driver(NEO4J_URI)
        driver.close()

    def test(self):
        self.assertEqual(1, 1)

    def tearDown(self):
        tt = 1

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()

