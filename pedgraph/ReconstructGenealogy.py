#!/usr/bin/env python3

from pedgraph.DataStructures import Record, Genealogy
from neo4j import GraphDatabase
import logging, argparse, os

logging.basicConfig(level=logging.INFO)

class ReconstructGenealogy(object):
    '''
    '''

    def __init__(self, uri, probands=None, probands_txt=None):
        '''
        Arguments:
        ----------
        uri :   String
            URI for the Python Neo4j driver to connect to the database.
        probands    :   List
            List of individual IDs.
        probands_txt    :   String
            Path to a file with individual IDs. One line per ID, with no header.
        '''

        assert probands is not None or probands_txt is not None, 'Error: neither ID list or file supplied.'
        assert probands is None or probands_txt is None, 'Error: ID list and ID file supplied. Supply only one.'

        # If the list of IDs is not supplied, we will read from the text file and construct the list.
        if probands is None:
            probands = []
            with open(probands_txt) as fid:
                for line in fid:
                    line = line.strip()  # Remove any leading or trailing whitespace.
                    inds.append(line)

        self.probands = probands

        # Connect to the database.
        self.driver = GraphDatabase.driver(uri)

        self.reconstruct_genealogy()

    def close(self):
        # Close the connection to the database.
        self.driver.close()


    def get_probands(self):
        '''
        '''

        with self.driver.session() as session:
            result = session.run('UNWIND $inds AS x MATCH (proband:Person {ind: x}) RETURN proband.ind', inds=self.probands)
            values = result.values()

        return values

    def get_ancestors(self):
        '''
        '''

        with self.driver.session() as session:
            result = session.run('UNWIND $inds AS x MATCH (proband:Person {ind: x})-[:is_child*]->(parent:Person)  '
                                  'RETURN DISTINCT parent.ind', inds=self.probands)
            values = result.values()

        # Flatten list.
        ancestors = [v[0] for v in values]

        return ancestors

    def get_records(self, inds):
        '''
        '''

        with self.driver.session() as session:
            result = session.run('UNWIND $inds AS x                                 '
                                 'MATCH (child:Person {ind: x})                     '
                                 'OPTIONAL MATCH (child)<-[:is_father]-(father)     '
                                 'OPTIONAL MATCH (child)<-[:is_mother]-(mother)     '
                                 'RETURN child.ind, father.ind, mother.ind, child.sex', inds=inds)
            values = result.values()

        return values


    def reconstruct_genealogy(self):


        # Check if all probands are found in database. Print a waring otherwise.
        # First query all probands in database.
        probands_in_db = self.get_probands()
        n_missing = len(probands_in_db) - len(self.probands)
        if n_missing > 0:
            logging.warning('Not all input probands found in database, %d missing.' % n_missing)

        logging.info('Reconstrucing genealogy of %d probands.' % len(self.probands))

        # Get a list of all ancestors of all probands.
        # This list is unique.
        ancestors = self.get_ancestors()

        logging.info('Found %d ancestors.' % len(ancestors))

        #ancestor_set = ancestor_set
        #proband_set = proband_set
        #unique_ancestors = ancestor_set.difference(proband_set)
        #ancestor_probands = ancestor_set.union(proband_set)
        #unique_ancestors = list(unique_ancestors)

        # Combine the list of probands with the list of ancestors.
        # There may be overlap between list of probands and ancestors, so we
        # find the union of the sets and convert back to a list.
        inds = list(set(ancestors).union(set(self.probands)))

        # Get a list of all records.
        records = self.get_records(inds)

        logging.info('Building a genealogy with %d individuals.' % len(records))

        # Initialize the Genealogy class to store the records in.
        gen = Genealogy()

        for rec in records:
            # Get fields from record.
            ind, father, mother, sex = rec

            # If mother/father relationships are not found, their IDs will be None.
            # Change these to "0" instead.
            if mother is None:
                mother = "0"
            if father is None:
                father = "0"

            # Construct a Record class for the current record.
            record = Record(ind, father, mother, sex)
            # Add record to genealogy.
            gen.add(record)

        logging.info('Number of individuals in reconstructed genealogy: %d' % gen.size)

        self.gen = gen



