#!/usr/bin/env python3

from neo4j import GraphDatabase
from pedgraph.BuildDB import BuildDB, AddNodeProperties, AddNodeLabels, AddNewNodes
from pedgraph.ReconstructGenealogy import ReconstructGenealogy
from pedgraph.DataStructures import Genealogy, Record
import unittest, logging
from os import environ as env

# Get the URI to
NEO4J_URI = env.get('NEO4J_URI')

TEST_PED_DOCKER = 'file:///test_tree.csv'

TEST_DATA_DIR = 'pedgraph/test/test_data/'
TEST_PED_LOCAL = TEST_DATA_DIR + 'test_tree.csv'
RECON_PED = TEST_DATA_DIR + 'test_tree_reconstructed.csv'

TEST_PROPERTIES_STRING = 'file:///str_properties.csv'
TEST_PROPERTIES_INT = 'file:///int_properties.csv'
TEST_PROPERTIES_FLOAT = 'file:///float_properties.csv'

TEST_LABELS = 'file:///label_nodes.csv'
TEST_NODES = 'file:///create_nodes.csv'

# If the NEO4J_URI environment variable is not defined, set it to the default.
if NEO4J_URI is None:
    NEO4J_URI = 'bolt://localhost:7687'

class TestSum(unittest.TestCase):

    def setUp(self):
        logging.info('Startup')
        logging.info('-------')

        self.driver = GraphDatabase.driver(NEO4J_URI)

        # Before we build, we wipe the database clean.
        with self.driver.session() as session:
            # Delete all nodes and their relationships.
            result = session.run('MATCH (n) DETACH DELETE n')

            # Delete all indexes.
            result = session.run('CALL db.indexes')
            values = result.values()

            index_uniqueness = [(v[1], v[4]) for v in values]
            for name, unique in index_uniqueness:
                if unique == 'NONUNIQUE':
                    logging.info('Deleting index %s.' % name)
                    result = session.run('DROP INDEX %s' % name)
                elif unique == 'UNIQUE':
                    logging.info('Deleting constraint %s.' % name)
                    result = session.run('DROP CONSTRAINT %s' % name)

        builder = BuildDB(NEO4J_URI, TEST_PED_DOCKER)

    def test_recon(self):
        logging.info('Reconstructing genealogy')
        logging.info('------------------------')

        # The reconstructed genealogy of these two individuals should contain
        # all individuals in the database.
        recon_gen = ReconstructGenealogy(NEO4J_URI, probands=['9', '10'])

        recon_gen.write_csv(RECON_PED)

        logging.info('Reading genealogy from CSV.')
        csv_gen = Genealogy()
        csv_gen.read_csv(TEST_PED_LOCAL)

        # Collect IDs from both genealogies.
        recon_inds = [rec.ind for rec in recon_gen]
        csv_inds = [rec.ind for rec in csv_gen]

        recon_inds_set = set(recon_inds)
        csv_inds_set = set(csv_inds)

        # Check that IDs are unique.
        self.assertTrue(len(recon_inds_set) == len(recon_inds), 'Error: duplicate individuals in reconstructed genealogy.')
        self.assertTrue(len(csv_inds_set) == len(csv_inds), 'Error: duplicate individuals in genealogy read from CSV.')

        # Check that there are no differences between the two ID sets.
        diff1 = recon_inds_set.difference(csv_inds_set)
        diff2 = csv_inds_set.difference(recon_inds_set)
        set_diff = diff1.union(diff2)
        self.assertTrue(len(set_diff) == 0, 'Error: individual IDs in reconstructed and CSV genealogies are not identical.')

        # Check that the rest of the information in the records matches.
        for recon_record in recon_gen:
            # For each record in the reconstructed genealogy.
            # Get the record ID.
            record_ind = recon_record.ind
            # Get the CSV record.
            csv_record = csv_gen.get(record_ind)

            self.assertTrue(recon_record.father == csv_record.father, 'Error: father does not match for record ind %s' % record_ind)
            self.assertTrue(recon_record.mother == csv_record.mother, 'Error: mother does not match for record ind %s' % record_ind)
            self.assertTrue(recon_record.sex == csv_record.sex, 'Error: sex does not match for record ind %s' % record_ind)

    def test_addprop(self):
        logging.info('Adding node property')
        logging.info('------------------------')

        # The expected values of the properties.
        # NOTE: these could also be read directly from the CSV
        # via a LOAD CSV query.
        inds = ['1', '2', '3', '4']
        str_props = ['a', 'b', 'a', 'b']
        str_props_dict = dict(zip(inds, str_props))
        int_props = [1, 2, 1, 0]
        int_props_dict = dict(zip(inds, int_props))
        float_props = [1.5, 2.1, 1.5, 0.5]
        float_props_dict = dict(zip(inds, float_props))

        logging.info('String property.')
        AddNodeProperties(NEO4J_URI, TEST_PROPERTIES_STRING, 'Person')

        logging.info('Integer property.')
        AddNodeProperties(NEO4J_URI, TEST_PROPERTIES_INT, 'Person', 'Integer')

        logging.info('Float property.')
        AddNodeProperties(NEO4J_URI, TEST_PROPERTIES_FLOAT, 'Person', 'Float')

        # Test that the int property matches the expected.
        # NOTE: could test all properties.
        with self.driver.session() as session:
            result = session.run('UNWIND $inds AS ind MATCH (p:Person {ind: ind}) RETURN p.ind, p.int_prop', inds=inds)
            values = result.values()
            for ind, prop in values:
                expected_prop = int_props_dict[ind]
                assert prop == expected_prop, 'Error: property for person %s does not match (expected "%s", got "%s")' %(ind, expected_prop, prop)

    def test_addlabel(self):
        logging.info('Adding node labels')
        logging.info('------------------------')

        # Person IDs.
        # NOTE: these could also be read directly from the CSV
        # via a LOAD CSV query.
        inds = ['1', '2', '3', '4']

        # Create a new label.
        new_label = 'TestLabel'
        AddNodeLabels(NEO4J_URI, TEST_LABELS, 'Person', new_label)

        with self.driver.session() as session:
            result = session.run('MATCH (p:%s) RETURN count(*)' % new_label)
            label_count = result.values()[0][0]
            assert label_count == len(inds), 'Error: expected %d labelled nodes but found %d.' %(len(inds), label_count)

    def test_addnodes(self):
        logging.info('Adding new node objects')
        logging.info('------------------------')

        # Fake RSIDs.
        # NOTE: these could also be read directly from the CSV
        # via a LOAD CSV query.
        rsid = ['1', '2', '3', '4']

        # Create a new label.
        new_label = 'Phenotype'
        AddNewNodes(NEO4J_URI, TEST_NODES, new_label)

        # Check that the number of created nodes matches the expected.
        with self.driver.session() as session:
            result = session.run('MATCH (p:%s) RETURN count(*)' % new_label)
            label_count = result.values()[0][0]
            assert label_count == len(rsid), 'Error: expected %d (:%s) nodes but found %d.' %(len(rsid), new_label, label_count)


    def tearDown(self):
        logging.info('-------')
        logging.info('Teardown')



if __name__ == '__main__':
    logging.basicConfig(level=logging.DEBUG)
    unittest.main()
