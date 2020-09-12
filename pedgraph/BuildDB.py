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

        # Count number of records in CSV.
        n_records = 0
        for _ in self.csv_reader():
            n_records += 1
        self.n_records = n_records

        # Populate database with nodes (people) and edges (relations).
        #self.populate_from_csv()
        self.load_csv()

        # Add labels to pedigree.
        self.label_founders()
        self.label_leaves()

        # Print some statistics.
        self.pedstats()

    def close(self):
        # Close the connection to the database.
        self.driver.close()

    def csv_reader(self):
        '''
        A generator that reads the CSV and yields records in `(ind, father, mother, sex)` tuples.
        '''
        with open(self.csv) as fid:
            if self.header:
                fid.readline()
            for line in fid:
                # Strip the line of potential whitespace.
                line = line.strip()

                # Split the line into fields.
                line = line.split(self.sep)

                # In case there are unnecessary fields we remove these.
                line = line[:4]

                # Get each field.
                ind, father, mother, sex = line

                # Strip fields in case there is whitespace surrounding.
                ind = ind.strip()
                father = father.strip()
                mother = mother.strip()
                sex = sex.strip()

                # Create a "Record" object.
                record = Record(ind, father, mother, sex)

                yield record

    def add_person(self, ind, sex):
        '''
        If a node with label `Person` and property `ind = [ ind ]` does not exist it will be created.
        Whether or not this node existed, we will give it the property `sex = [ sex ]`.
        '''

        # TODO: consider whether to log everytime a node or edge is created, and when properties
        # or labels are added to nodes or edges.
        with self.driver.session() as session:
            result = session.run("MERGE (person:Person {ind: $ind}) "
                                 "SET person.sex = $sex", ind=ind, sex=sex)
        return result

    def add_child(self, child, parent):
        '''
        Add a "child" relationship. Steps:
        * Find the `child` node
        * If `parent` does not exist, create it
        * Make a `[:is_child]` relation from `child` to `parent`

        If `parent` is `na_id`, no relation nor node will be added.
        '''

	# ID '0' means the person does not exist. Relationship will not be added.
        if parent == self.na_id:
            return None

        with self.driver.session() as session:
            result = session.run("MATCH (child:Person {ind: $child})        "
                                 "MERGE (parent:Person {ind: $parent})      "
                                 "MERGE (child)-[:is_child]->(parent)       ", child=child, parent=parent)
        return result

    def add_parent(self, child, parent, relation):
        '''
        Add a "parent" relationship. Steps:
        * Find the `child` node
        * If `parent` does not exist, create it
        * Make a relation from `parent` to `child`

        If `parent` is `na_id`, no relation nor node will be added. The relation of `parent` to `child` is one of either
        `is_parent`, `is_mother`, or `is_father`.
        '''

        assert relation in ['parent', 'mother', 'father'], 'Error: "relation" must be one of: "parent", "mother", or "father".'

	# ID '0' means the person does not exist. Relationship will not be added.
        if parent == self.na_id:
            return None

        with self.driver.session() as session:
            result = session.run("MATCH (child:Person {ind: $child})        "
                                 "MERGE (parent:Person {ind: $parent })     "
                                 "MERGE (child)<-[:is_%s]-(parent)          " % relation, child=child, parent=parent, relation=relation)
        return result

    def populate_from_csv(self):
        '''
        Make a node for each individual, and make all relations. The relations made are of the type:
        * `[:is_child]`
        * `[:is_mother]`
        * `[:is_father]`
        * `[:is_parent]`
        '''

        csv_reader = self.csv_reader()
        logging.info('Building database')
        # Using tqdm progress bar.
        with tqdm(total=self.n_records, desc='Progress') as pbar:
            for record in tqdm(csv_reader):
                ind = record.ind
                father = record.father
                mother = record.mother
                sex = record.sex

                # Add person to database, labelling the ID and sex.
                self.add_person(ind, sex)
                # Add child relationships.
                self.add_child(ind, mother)
                self.add_child(ind, father)
                ## Add mother and father relationships.
                self.add_parent(ind, mother, 'mother')
                self.add_parent(ind, father, 'father')
                ## Add parent relationships as well.
                self.add_parent(ind, mother, 'parent')
                self.add_parent(ind, father, 'parent')

                # Increment progress bar iteration counter.
                pbar.update()

    def load_csv(self):
        '''
        Make a node for each individual, and make all relations. The relations made are of the type:
        * `[:is_child]`
        * `[:is_mother]`
        * `[:is_father]`
        * `[:is_parent]`
        '''

        with driver.session() as session:
            result = session.run("LOAD CSV WITH HEADERS FROM $csv_file AS line      "
                                 "MERGE (person:Person {ind: line.ind})             "
                                 "SET person.sex = $sex                             "
                                 "MERGE (father:Person {ind: $line.father})         "
                                 "MERGE (mother:Person {ind: $line.mother})         "
                                 "MERGE (person)-[:is_child]->(father)              "
                                 "MERGE (person)-[:is_child]->(mother)              "
                                 "MERGE (father)-[:is_father]->(person)             "
                                 "MERGE (mother)-[:is_mother]->(person)             ", csv_file=self.csv)

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
