from neo4j import GraphDatabase
import logging, argparse

logging.basicConfig(level=logging.INFO)

class BuildDB(object):
    '''
    The constructor connects to the Neo4j database and populates it with people and relations. It
    also checks that there are not duplicate individuals.
    '''

    def __init__(self, uri, csv, header=True, sep=','):
        '''
        Arguments:
        ----------
        uri :   String
            URI for the Python Neo4j driver to connect to the database.
        csv :   String
            Path to CSV pedigree file.
        header  :   Boolean
            Whether the CSV has a header line or not.
        sep :   String
            Separator used in the CSV file.
        '''
        self.csv = csv
        self.header = header
        self.sep = sep

        # Connect to the database.
        self.driver = GraphDatabase.driver(uri)

        # An error will be raised if there are duplicate IDs.
        self.assert_unique_inds()

        # Populate database with nodes (people) and edges (relations).
        self.populate_from_csv()

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

                yield (ind, father, mother, sex)

    def assert_unique_inds(self):
        csv_reader = self.csv_reader()
        # Get all the "ind" records in a list.
        inds = [record[0] for record in csv_reader]
        # This will raise an error if there are duplicate IDs.
        assert len(inds) == len(set(inds)), 'Error: individual IDs in CSV are not unique: %s' % csv

    def add_person(self, ind, sex):
        '''
        If a node with label `Person` and property `ind = [ ind ]` does not exist it will be created.
        Whether or not this node existed, we will give it the property `sex = [ sex ]`.
        '''
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

        If `parent` is 0, no relation nor node will be added.
        '''

        if parent == '0':
            logging.info('Parent is 0 (does not exist). Will not update database.')
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

        If `parent` is 0, no relation nor node will be added. The relation of `parent` to `child` is one of either
        `is_parent`, `is_mother`, or `is_father`.
        '''

        assert relation in ['parent', 'mother', 'father'], 'Error: "relation" must be one of: "parent", "mother", or "father".'

        if parent == '0':
            logging.info('Parent is 0 (does not exist). Will not update database.')
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
        for record in csv_reader:
            ind, father, mother, sex = record
            # Add person to database, labelling the ID and sex.
            self.add_person(ind, sex)
            self.add_child(ind, mother)
            ## Add mother and father relationships.
            self.add_parent(ind, mother, 'mother')
            self.add_parent(ind, father, 'father')
            ## Add parent relationships as well.
            self.add_parent(ind, mother, 'parent')
            self.add_parent(ind, father, 'parent')



if __name__ == "__main__":
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(description='Build database, populating it with individuals and relations from a pedigree CSV.')

    # Arguments for parser.
    parser.add_argument('--uri', type=str, required=True, help='URI for the Python Neo4j driver to connect to the database.')
    parser.add_argument('--csv', type=str, required=True, help='Path to CSV pedigree file.')

    # Parse input arguments.
    args = parser.parse_args()

    # Call the class to build the database.
    build_db = BuildDB(args.uri, args.csv)
    # Close the connection to the database.
    build_db.close()
