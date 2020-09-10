#!/usr/bin/env python3

import logging

logging.basicConfig(level=logging.INFO)

class Record(object):
    '''
    Class defines all essential information for a single individual. The option
    to leave arguments as `None` is reserved only for when partial information
    is added first, and remaining information is adder at a later stage.

    ind :   String
    father  :   String
    mother  :   String
    sex :   String
    '''
    def __init__(self, ind=None, father=None, mother=None, sex=None):
        self.ind = ind
        self.father = father
        self.mother = mother
        self.sex = sex

class Genealogy(object):
    '''
    Defines a set of individuals in a genealogy.
    '''

    def __init__(self):
        # This dictionary holds individual IDs as keys and Record objects as values.
        self.data = dict()
        self.size = 0  # Number of individuals in genealogy.

    def __setitem__(self, key, value):
        # If the key already exists in the dictionary, the value will be overwritten, and the
        # size of the genealogy will not be updated.
        overwrite = False
        if self.get(key) is not None:
            logging.warning('Record with ind=%s already exists. Will be over-written.' % key)
            overwrite = True

        self.data[key] = value

        if not overwrite:
            self.size += 1

    def __getitem__(self, key):
        return self.data.get(key)

    def __delitem__(self, key):
        self.data.__delitem__(key)
        self.size -= 1

    def get(self, ind):
        return self[ind]

    def add(self, record):
        self[record.ind] = record

    def __iter__(self):
        '''
        Generator that yields all record objects in genealogy.
        '''
        for record in self.data.values():
            yield record

    def write_csv(self, csv_path, header=True, sep=','):
        '''
        Write all records in genealogy to a CSV file, in a random order.

        csv_path    :   String
            Path to CSV pedigree file.
        header  :   Boolean
            Whether the CSV has a header line or not.
        sep :   String
            Separator used in the CSV file.
        '''
        with open(csv_path, 'w') as fid:
            fid.write('ind,father,mother,sex\n')
            for record in self:
                row = '%s,%s,%s,%s' %(record.ind, record.father, record.mother, record.sex)
                fid.write(row + '\n')
