from pedgraph.datastructures import Record
from neo4j import GraphDatabase
import logging, argparse, os

logging.basicConfig(level=logging.INFO)

class WriteCSV(object):
    '''
    The constructor connects to the Neo4j database, all individuals, their mother and father
    relationships, their sex, and writes this to a CSV.
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

        assert not os.path.isfile(self.csv), 'Error: file "%s" already exists.' % self.csv

        # Connect to the database.
        self.driver = GraphDatabase.driver(uri)

        # Write all records to CSV.
        self.write_csv()

    def close(self):
        # Close the connection to the database.
        self.driver.close()

    def get_persons(self):
        '''
        Returns a list of `{'ind': [ ind ], 'sex': [ sex ]}` dictionaries.
        '''

        with self.driver.session() as session:
            result = session.run("MATCH (p:Person) RETURN p.ind AS ind, p.sex as sex")
            data = result.data()

        return data

    def get_trios(self):
        '''
        Returns a list of `{'ind': [ ind ], 'father': [ father ], 'mother': [ mother ]}` dictionaries.
        '''

        with self.driver.session() as session:
            result = session.run("MATCH (mother:Person)-[:is_mother]->(ind:Person)<-[:is_father]-(father:Person)    "
                                 "RETURN ind.ind AS ind, father.ind AS father, mother.ind AS mother                 ")
            data = result.data()

        return data

    def get_child_father(self):
        '''
        Returns a list of `{'ind': [ ind ], 'father': [ father ]}` dictionaries.
        '''

        with self.driver.session() as session:
            result = session.run("MATCH (father:Person)-[:is_father]->(ind:Person)  "
                                 "RETURN ind.ind AS ind, father.ind AS father       ")
            data = result.data()

        return data

    def get_child_mother(self):
        '''
        Returns a list of `{'ind': [ ind ], 'mother': [ mother ]}` dictionaries.
        '''

        with self.driver.session() as session:
            result = session.run("MATCH (mother:Person)-[:is_mother]->(ind:Person)  "
                                 "RETURN ind.ind AS ind, mother.ind AS mother       ")
            data = result.data()

        return data

    def write_csv(self):
        '''
        Write all persons and their parents to CSV. This method retrieves all persons and their sex,
        and retrieves all mother-child and father-child relationships. Parents that are missing are
        written as "0".
        '''
        # Get individual IDs and their sex.
        persons = self.get_persons()
        logging.info('%d persons found.' % len(persons))

        # Get all child-mother relationships.
        mothers = self.get_child_mother()
        logging.info('%d child-mother relationships found.' % len(mothers))

        # Get all child-father relationships.
        fathers = self.get_child_father()
        logging.info('%d child-father relationships found.' % len(fathers))

        # Make dictionary such that we can look-up fathers and mothers.
        father_dict = {item['ind']: item for item in fathers}
        mother_dict = {item['ind']: item for item in mothers}

        records = []
        for item in persons:
            # Get ID and sex of person.
            ind = item['ind']
            sex = item['sex']

            # Look-up parents in dictionaries.
            father_item = father_dict.get(ind)
            mother_item = mother_dict.get(ind)

            # If the parents don't exist, they are equal to None, and in that case
            # we set them to 0.
            if father_item:
                father = father_item['father']
            else:
                father = '0'
            if mother_item:
                mother = mother_item['mother']
            else:
                mother = '0'

            records.append(Record(ind, father, mother, sex))

        with open(self.csv, 'w') as fid:
            fid.write('ind,father,mother,sex\n')
            for record in records:
                row = '%s,%s,%s,%s' %(record.ind, record.father, record.mother, record.sex)
                fid.write(row + '\n')



if __name__ == "__main__":
    # Parse command-line arguments.
    parser = argparse.ArgumentParser(description='Read records from the database and write to a CSV.')

    # Arguments for parser.
    parser.add_argument('--uri', type=str, required=True, help='URI for the Python Neo4j driver to connect to the database.')
    parser.add_argument('--csv', type=str, required=True, help='Path to CSV file to write the pedigree.')

    # Parse input arguments.
    args = parser.parse_args()

    # Call the class to build the database.
    build_db = WriteCSV(args.uri, args.csv)
    # Close the connection to the database.
    build_db.close()
