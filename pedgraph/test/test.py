
# TODO:
# Write to DB, read from DB, write to CSV, and compare with original CSV.

from pedgraph.BuildDB import BuildDB
from pedgraph.WriteCSV import WriteCSV
import unittest, logging
from os import environ as env


class TestSum(unittest.TestCase):

    def setUp(self):
        builder = BuildDB(, )
        writer = WriteCSV(, )

    def test(self):
        self.assertEqual()

if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    db_uri = env['NEO4J_BOLT_URI']
    csv_orig = env['PED_TEST_CSV']
    csv_rewrite = env['PED_REWRITE_CSV']
    unittest.main()

