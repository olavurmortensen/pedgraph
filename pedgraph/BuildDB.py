#!/usr/bin/env python3

from pedgraph.DataStructures import Record
from neo4j import GraphDatabase
import logging, argparse
from tqdm.auto import tqdm

logging.basicConfig(level=logging.INFO)

class BuildDB(object):
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

        # Connect to the database.
        self.driver = GraphDatabase.driver(uri)

        # Populate database with nodes (people) and edges (relations).
        self.load_csv()
# Add labels to pedigree.  self.label_founders() self.label_leaves()

        # Print some statistics.
        self.pedstats()

    def close(self):
        # Close the connection to the database.
        self.driver.close()

    def load_csv(self):
        '''
        Make a node for each individual, and make all relations. The relations made are of the type:
        * `[:is_child]`
        * `[:is_mother]`
        * `[:is_father]`
        * `[:is_parent]`
        '''

        with self.driver.session() as session:
            result = session.run("LOAD CSV WITH HEADERS FROM $csv_file AS line      "
                                 "USING PERIODIC COMMIT 1000                        "
                                 "MERGE (person:Person {ind: line.ind})             "
                                 "SET person.sex = line.sex                         "
                                 "MERGE (father:Person {ind: line.father})         "
                                 "MERGE (mother:Person {ind: line.mother})         "
                                 "MERGE (person)-[:is_child]->(father)              "
                                 "MERGE (person)-[:is_child]->(mother)              "
                                 "MERGE (father)-[:is_father]->(person)             "
                                 "MERGE (mother)-[:is_mother]->(person)             "
                                 "RETURN line                                       ", csv_file=self.csv)

            # Detach all connections to non-existent parent "na_id", and delete the "na_id" node.
            # NOTE: this could be avoided using some sort of if-statement above, but this is so easy.
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
