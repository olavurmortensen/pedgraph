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

    def get_csv_header(self):
        with self.driver.session() as session:
            # Read the header of the file, i.e. the first row.
            result = session.run('LOAD CSV FROM $csv AS line RETURN line LIMIT 1', csv=self.csv)
            header = result.values()[0][0]
        return header

    def check_csv_basic(self, csv_columns=None):
        '''
        Check that the CSV (with headers) is not empty, and check that it contains all the necessary
        columns (if specified). Also check that Neo4j is able to access the CSV file.
        '''

        header = self.get_csv_header()

        logging.info('Loaded CSV with header: ' + ','.join(header))

        if csv_columns is not None:
            # Make sure all the needed fields are in the CSV.
            for field in csv_columns:
                assert field in header, 'Error: CSV does not contain "%s" field.' % field

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


    def create_index(self, index_name, node_label, node_property, unique=False):
        '''
        Create an index on a property on nodes with a specific label, optionally with a
        uniqueness constraint. Indexing has huge performance benefits.

        If an equivalent index already exists, or an index with the same name, nothing
        will be done.

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

        # When trying to create index/constraint, these errors will be ignored and no index will be created.
        catch_errors = ['EquivalentSchemaRuleAlreadyExists', 'ConstraintAlreadyExists', 'ConstraintWithNameAlreadyExists']

        # The index does not exist, so we will create it.
        with self.driver.session() as session:
            # Create an index on "ind" and constain it to be unique.
            try:
                if unique:
                    # Create an index with a uniqueness constraint.
                    result = session.run('CREATE CONSTRAINT %s ON (p:%s) '
                                         'ASSERT p.%s IS UNIQUE'
                                         % (index_name, node_label, node_property))
                else:
                    # Create an index.
                    result = session.run('CREATE INDEX %s FOR (p:%s) ON (p.%s)'
                                         % (index_name, node_label, node_property))
            except ClientError as err:
                if err.title in catch_errors:
                    logging.info('An index on (:%s {%s}) already exists, will not create.' %(node_label, node_property))
                else:
                    raise
            finally:
                logging.info('Created an index "%s" on (:%s {%s}) nodes.' % (index_name, node_label, node_property))


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


class AddNodeProperties(BaseBuilder):
    '''
    Add properties to nodes from a CSV file.
    '''

    def __init__(self, uri, csv, node_label, prop_type='String', index_unique=False):
        '''
        Arguments:
        ----------
        uri :   String
            URI for the Python Neo4j driver to connect to the database.
        csv :   String
            Path to CSV file. The name of the columns correspond to properties; the first column
            is the property to match, and a property will be created with the name of the second
            column.
        node_label  :   String
            Label of nodes to add property to. E.g. `Person` for `(:Person)` nodes.
        prop_type   :   String
            Data type of property, one of: 'String', 'Integer' or 'Float'.
        index_unique    :   Boolean
            Whether or not to create a uniqueness constraint on property.
        '''
        self.csv = csv
        self.node_label = node_label
        self.prop_type = prop_type
        self.index_unique = index_unique

        self.prop_types = ['String', 'Integer', 'Float']
        assert prop_type in self.prop_types, 'Error: "prop_type" must be one of: ' + ', '.join(self.prop_types)

        # Connect to the database.
        self.driver = GraphDatabase.driver(uri)

        # Check CSV file.
        self.check_csv_basic()

        # Get the header of the CSV file.
        self.header = self.get_csv_header()

        # The property to match the individuals by should be the first columns.
        # The property to create should be the second.
        self.prop_match, self.prop_name = self.header

        # Populate database with nodes (people) and edges (relations).
        self.load_csv()

        # Print some statistics.
        self.print_stats()

        self.close()

    def load_csv(self):
        '''
        Match nodes by the first columns of the CSV file, and set the property
        provided in the second column.
        '''

        prop_match = self.prop_match
        prop_name = self.prop_name
        node_label = self.node_label

        with self.driver.session() as session:

            # Count the number of nodes that match.
            result = session.run('LOAD CSV WITH HEADERS FROM $csv AS line               '
                    'MATCH (:%s {%s: line.%s}) RETURN count(*)' %(node_label, prop_match, prop_match), csv=self.csv)

            n_matches = result.values()[0][0]

            # Count number of lines in file.
            result = session.run('LOAD CSV WITH HEADERS FROM $csv AS line           '
                    'RETURN count(*)', csv=self.csv)

            n_rows = result.values()[0][0]

            logging.info('%d out of %d rows in census CSV matched a record.' % (n_matches, n_rows))

            # Create an index on the property. If this property has been used before, the index will
            # alredy exist, and the call below will do nothing.
            index_name = 'index_' + prop_name
            self.create_index(index_name, node_label, prop_name, self.index_unique)

            # Match all the nodes and add the properties.
            result = session.run("USING PERIODIC COMMIT 1000                        "
                    "LOAD CSV WITH HEADERS FROM $csv AS line      "
                    "MATCH (node:%s {%s: line.%s})             "
                    "SET node.%s = to%s(line.%s)   "
                    "RETURN count(*)                                 "
                    %(node_label, prop_match, prop_match, prop_name, self.prop_type, prop_name),
                    csv=self.csv)

    def print_stats(self):
        with self.driver.session() as session:
            result = session.run('MATCH (p:%s) WHERE EXISTS (p.%s) RETURN p.%s' %(self.node_label, self.prop_name, self.prop_name), csv=self.csv)
            values = result.values()

        n_match = len(values)
        prop_list = [v[0] for v in values]
        prop_unique = set(prop_list)
        logging.info('Added %d unique property values to %d different nodes.' % (len(prop_unique), n_match))


class AddNodeLabels(BaseBuilder):
    '''
    Add labels to nodes from a CSV file.
    '''

    def __init__(self, uri, csv, match_label, new_label):
        '''
        Arguments:
        ----------
        uri :   String
            URI for the Python Neo4j driver to connect to the database.
        csv :   String
            Path to CSV file. The name of the first (and only) column should be the name
            of the property to match.
        match_label  :   String
            Label of nodes to match. E.g. `Person` for `(:Person)` nodes.
        new_label  :   String
            Label to create, for example `Proband` to create `(:Person:Proband)` nodes.
        '''
        self.csv = csv
        self.match_label = match_label
        self.new_label = new_label

        # Connect to the database.
        self.driver = GraphDatabase.driver(uri)

        # Check CSV file.
        self.check_csv_basic()

        # Get the header of the CSV file.
        self.header = self.get_csv_header()

        # The property to match the individuals by should be the first column.
        self.prop_match = self.header[0]

        # Populate database with nodes (people) and edges (relations).
        self.load_csv()

        self.close()

    def load_csv(self):
        '''
        '''

        prop_match = self.prop_match
        match_label = self.match_label
        new_label = self.new_label

        with self.driver.session() as session:

            # Count the number of nodes that match.
            result = session.run('LOAD CSV WITH HEADERS FROM $csv AS line               '
                    'MATCH (:%s {%s: line.%s}) RETURN count(*)' %(match_label, prop_match, prop_match), csv=self.csv)

            n_matches = result.values()[0][0]

            # Count number of lines in file.
            result = session.run('LOAD CSV WITH HEADERS FROM $csv AS line           '
                    'RETURN count(*)', csv=self.csv)

            n_rows = result.values()[0][0]

            logging.info('%d out of %d rows in census CSV matched a record.' % (n_matches, n_rows))

            # Match all the nodes and add the properties.
            result = session.run("USING PERIODIC COMMIT 1000                        "
                    "LOAD CSV WITH HEADERS FROM $csv AS line      "
                    "MATCH (node:%s {%s: line.%s})             "
                    "SET node:%s                     "
                    "RETURN count(*)                                 "
                    %(match_label, prop_match, prop_match, new_label),
                    csv=self.csv)

            result = session.run('MATCH (p:%s) RETURN count(*)' % new_label)
            labels_created = result.values()[0][0]
            logging.info('Added label "%s to %d nodes.' % (new_label, labels_created))

class AddNewNodes(BaseBuilder):
    '''
    Add new nodes from a CSV file.
    '''

    def __init__(self, uri, csv, node_label):
        '''
        Arguments:
        ----------
        uri :   String
            URI for the Python Neo4j driver to connect to the database.
        csv :   String
            Path to CSV file. The name of the first (and only) column of the CSV will
            become the unique ID of the nodes.
        node_label  :   String
            Label of the nodes. E.g. `Phenotype` for `(:Phenotype)` nodes.
        '''
        self.csv = csv
        self.node_label = node_label

        # Connect to the database.
        self.driver = GraphDatabase.driver(uri)

        # Check CSV file.
        self.check_csv_basic()

        # Get the header of the CSV file.
        self.header = self.get_csv_header()

        # The property to match the individuals by should be the first columns.
        # The property to create should be the second.
        self.id_name = self.header[0]

        # Populate database with nodes (people) and edges (relations).
        self.load_csv()

        self.close()

    def load_csv(self):
        '''
        '''

        id_name = self.id_name
        node_label = self.node_label

        with self.driver.session() as session:

            # Count the number of nodes that match.
            result = session.run('LOAD CSV WITH HEADERS FROM $csv AS line               '
                    'MATCH (:%s {%s: line.%s}) RETURN count(*)' %(node_label, id_name, id_name), csv=self.csv)

            n_matches = result.values()[0][0]

            logging.info('%d records in the CSV already match a node.' % n_matches)

            # Count number of lines in file.
            result = session.run('LOAD CSV WITH HEADERS FROM $csv AS line           '
                    'RETURN count(*)', csv=self.csv)

            n_rows = result.values()[0][0]

            logging.info('Reading from CSV with %d rows.' % n_rows)

            # Create an index and a uniqueness constraint on the property. If this property has been used
            #before, the index will alredy exist, and the call below will do nothing.
            index_name = 'index_' + id_name
            self.create_index(index_name, node_label, id_name, True)

            # Match all the nodes and add the properties.
            result = session.run("USING PERIODIC COMMIT 1000                        "
                    "LOAD CSV WITH HEADERS FROM $csv AS line      "
                    "MERGE (node:%s {%s: line.%s})             "
                    "RETURN count(*)                                 "
                    %(node_label, id_name, id_name),
                    csv=self.csv)

            # Forcing evaluation of above query.
            _ = result.values()



