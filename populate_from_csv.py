from neo4j import GraphDatabase
import logging, argparse

logging.basicConfig(level=logging.INFO)

class PopulateFromCsv(object):

    def __init__(self, uri, csv, header=True, sep=','):
        self.driver = GraphDatabase.driver(uri)
        self.csv = csv
        self.header = True
        self.sep = ','
        self.assert_unique_inds()

    def close(self):
        self.driver.close()

    def add_person(self, ind, sex):
        with self.driver.session() as session:
            result = session.run("MERGE (person:Person {ind: $ind}) "
                                 "SET person.sex = $sex", ind=ind, sex=sex)
        return result

    def add_child(self, child, parent):

        if parent == '0':
            logging.info('Parent is 0 (does not exist). Will not update database.')
            return None

        with self.driver.session() as session:
            result = session.run("MATCH (child:Person {ind: $child})    "
                                 "MERGE (child)-[:is_child]->(:Person {ind: $parent})", child=child, parent=parent)
        return result

    def add_parent(self, child, parent, relation):

        assert relation in ['parent', 'mother', 'father'], 'Error: "relation" must be one of: "parent", "mother", or "father".'

        if parent == '0':
            logging.info('Parent is 0 (does not exist). Will not update database.')
            return None

        with self.driver.session() as session:
            result = session.run("MATCH (child:Person {ind: $child})    "
                                 "MERGE (child)<-[:is_%s]-(:Person {ind: $parent})" % relation, child=child, parent=parent, relation=relation)
        return result

    def csv_reader(self):
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

                yield (ind, father, mother, sex)

    def assert_unique_inds(self):
        csv_reader = self.csv_reader()
        inds = [record[0] for record in csv_reader]
        assert len(inds) == len(set(inds)), 'Error: individual IDs in CSV are not unique: %s' % csv

    def populate_from_csv(self):
        csv_reader = self.csv_reader()
        for record in csv_reader:
            ind, father, mother, sex = record
            # Add person to database, labelling the ID and sex.
            self.add_person(ind, sex)
            #self.add_child(ind, mother)
            # Add mother and father relationships.
            self.add_parent(ind, mother, 'mother')
            #self.add_parent(ind, father, 'father')
            # Add parent relationships as well.
            #self.add_parent(ind, mother, 'parent')
            #self.add_parent(ind, father, 'parent')



if __name__ == "__main__":
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(description='Populate database with information from CSV.')

    # Arguments for parser.
    parser.add_argument('--uri', type=str, required=True, help='URI for the Python Neo4j driver to connect to the database.')
    parser.add_argument('--csv', type=str, required=True, help='Path to CSV pedigree file.')

    # Parse input arguments.
    args = parser.parse_args()

    # Initialize the class.
    populate = PopulateFromCsv(args.uri, args.csv)
    # Populate the database.
    populate.populate_from_csv()
    # Close the connection to the database.
    populate.close()

