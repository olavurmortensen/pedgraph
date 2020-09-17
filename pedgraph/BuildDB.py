#!/usr/bin/env python3

from pedgraph.DataStructures import Record
from neo4j import GraphDatabase
from neo4j.exceptions import ClientError
import logging, argparse
from tqdm.auto import tqdm

logging.basicConfig(level=logging.INFO)


class BaseBuilder(object):
    '''
    This base-class contains some methods for basic functionality for building the
    database.
    '''

    def __init__(self, uri, csv):
        '''
        Arguments:
        ----------
        uri :   String
            URI for the Python Neo4j driver to connect to the database.
        csv :   String
            Path to CSV file.
        '''
        self.csv = csv

        # Connect to the database.
        self.driver = GraphDatabase.driver(uri)

    def close(self):
        # Close the connection to the database.
        self.driver.close()

    def check_csv_basic(self, csv_columns):
        '''
        Check that the CSV (with headers) is not empty, and check that it contains all the necessary
        columns. Also check that Neo4j is able to access the CSV file.
        '''
        # Read the entire CSV using a database query and return all lines.
        with self.driver.session() as session:
            result = session.run('LOAD CSV WITH HEADERS FROM $csv AS line RETURN line', csv=self.csv)
            # If the CSV can't be read, show an error log and raise the error.
            try:
                values = result.values()
            except ClientError:
                logging.error('CSV file could not be loaded into Neo4j: %s' % self.csv)
                raise

        assert len(values) > 0, 'Error: reading CSV returned 0 lines.'

        # Get a single row.
        single = values[0][0]
        # Get the keys, corresponding to fields in the CSV.
        keys = list(single.keys())

        logging.info('Loaded CSV with columns: %s' % ','.join(keys))

        # Make sure all the needed fields are in the CSV.
        for field in csv_columns:
            assert field in keys, 'Error: CSV does not contain "%s" field.' % field

    def create_index(self, index_name, node_label, node_property, unique=False):
        '''
        Create an index on a property on nodes with a specific label, optionally with a
        uniqueness constraint. Indexing has huge performance benefits.

        Arguments:
        ----------
        index_name  :   String
            Name of index.
        node_label  :   String
            Index nodes labelled `(:node_label)`
        node_property:   String
            Index property `({node_property})`
        unique  :   Boolean
            Whether or not to create a uniqueness constraint on property `node_property`.
        '''
        # The index does not exist, so we will create it.
        with self.driver.session() as session:
            # Create an index on "ind" and constain it to be unique.
            try:
                if unique:
                    # Create an index with a uniqueness constraint.
                    result = session.run('CREATE CONSTRAINT %s ON (p:%s) '
                                         'ASSERT p.%s IS UNIQUE'
                                         % (index_name, node_label, node_property))
                    logging.info('Creating an index "%s" on individual IDs.' % index_name)
                else:
                    # Create an index.
                    result = session.run('CREATE INDEX %s FOR (p:%s) ON (p.%s)'
                                         % (index_name, node_label, node_property))
            except ClientError as err:
                if err.title == 'ConstraintAlreadyExists':
                    logging.info('An index on (:%s {%s}) already exists, will not create.' %(node_label, node_property))
                else:
                    raise


class BuildDB(BaseBuilder):
    '''
    The constructor connects to the Neo4j database and populates it with people and relations. It
    also checks that there are not duplicate individuals.
    '''

    def __init__(self, uri, csv, na_id='0'):
        '''
        Arguments:
        ----------
        uri :   String
            URI for the Python Neo4j driver to connect to the database.
        csv :   String
            Path to CSV pedigree file.
        na_id :   String
            ID used for missing parents.
        '''
        self.csv = csv
        self.na_id = na_id

        CSV_COLUMNS = ['ind', 'father', 'mother', 'sex']

        # Connect to the database.
        self.driver = GraphDatabase.driver(uri)

        # Check CSV file.
        self.check_csv_basic(CSV_COLUMNS)

        # Create an index an uniqueness constraint on 'ind'.
        self.create_index('index_ind', 'Person', 'ind', unique=True)

        # Populate database with nodes (people) and edges (relations).
        self.load_csv()

        # Add labels to pedigree.
        logging.info('Labelling founder nodes.')
        self.label_founders()
        logging.info('Labelling leaf nodes.')
        self.label_leaves()

        # Print some statistics.
        self.pedstats()

        self.close()

    def load_csv(self):
        '''
        Make a node for each individual, and make all relations. The relations made are of the type:
        * `[:is_child]`
        * `[:is_mother]`
        * `[:is_father]`
        * `[:is_parent]`
        '''

        with self.driver.session() as session:
            result = session.run("USING PERIODIC COMMIT 1000                        "
                                 "LOAD CSV WITH HEADERS FROM $csv_file AS line      "
                                 "MERGE (person:Person {ind: line.ind})             "
                                 "SET person.sex = line.sex                         "
                                 "MERGE (father:Person {ind: line.father})         "
                                 "MERGE (mother:Person {ind: line.mother})         "
                                 "MERGE (person)-[:is_child]->(father)              "
                                 "MERGE (person)-[:is_child]->(mother)              "
                                 "MERGE (father)-[:is_father]->(person)             "
                                 "MERGE (mother)-[:is_mother]->(person)             ", csv_file=self.csv)

            # Detach all connections to non-existent parent "na_id", and delete the "na_id" node.
            # NOTE: this could be avoided using some sort of if-statement above, but this is so easy.
            logging.info('Detaching and deleting "null" node ind=%s.' % self.na_id)
            result = session.run('MATCH (p {ind: $na_id}) DETACH DELETE p', na_id=self.na_id)

    def label_founders(self):
        '''
        Find founders and add labels. For each person that does not have a parent defined by a `is_child` relationship,
        add a new label `:Founder`. The new label does not overwrite the existing label(s).
        '''
        with self.driver.session() as session:
            result = session.run('MATCH (p:Person) WHERE NOT (p)-[:is_child]->() SET p:Founder')

    def label_leaves(self):
        '''
        Find leaves and add labels. For each person that does not have a child defined by a `is_child` relationship,
        add a new label `:Leaf`. The new label does not overwrite the existing label(s).
        '''
        with self.driver.session() as session:
            result = session.run('MATCH (p:Person) WHERE NOT (p)<-[:is_child]-() SET p:Leaf')

    def pedstats(self):
        with self.driver.session() as session:
            logging.info('NODE STATS')

            result = session.run('MATCH (p:Person) RETURN p')
            logging.info('#Persons: %d' % len(result.values()))

            result = session.run('MATCH (p:Person {sex: "F"}) RETURN p')
            logging.info('#Females: %d' % len(result.values()))

            result = session.run('MATCH (p:Person {sex: "M"}) RETURN p')
            logging.info('#Males: %d' % len(result.values()))

            result = session.run('MATCH (p:Founder) RETURN p')
            logging.info('#Founders: %d' % len(result.values()))

            result = session.run('MATCH (p:Leaf) RETURN p')
            logging.info('#Leaves: %d' % len(result.values()))

            logging.info('EDGE STATS')

            result = session.run('MATCH ()-[r:is_child]->() RETURN r')
            logging.info('#is_child: %d' % len(result.values()))

            result = session.run('MATCH ()-[r:is_mother]->() RETURN r')
            logging.info('#is_mother: %d' % len(result.values()))

            result = session.run('MATCH ()-[r:is_father]->() RETURN r')
            logging.info('#is_father: %d' % len(result.values()))




if __name__ == "__main__":
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(description='Build database, populating it with individuals and relations from a pedigree CSV.')

    # Arguments for parser.
    parser.add_argument('--uri', type=str, required=True, help='URI for the Python Neo4j driver to connect to the database.')
    parser.add_argument('--csv', type=str, required=True, help='Path to CSV pedigree file.')
    parser.add_argument('--header', type=bool, required=False, default=True, help='Whether or not the CSV has a header.')
    parser.add_argument('--sep', type=str, required=False, default=',', help='Separator used in the CSV.')
    parser.add_argument('--na_id', type=str, required=False, default='0', help='The ID used for missing parents.')

    # Parse input arguments.
    args = parser.parse_args()

    # Call the class to build the database.
    build_db = BuildDB(args.uri, args.csv, args.header, args.sep, args.na_id)
    # Close the connection to the database.
    build_db.close()
