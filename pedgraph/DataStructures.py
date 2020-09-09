#!/usr/bin/env python3

import logging

logging.basicConfig(level=logging.INFO)

class Record(object):
    def __init__(self, ind=None, father=None, mother=None, sex=None):
        self.ind = ind
        self.father = father
        self.mother = mother
        self.sex = sex

class Genealogy(object):
    def __init__(self):
        self.data = dict()
        self.size = 0

    def __setitem__(self, key, value):
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

    def write_csv(self):
        # Might be a good idea to have the code for writing CSV here instead.
        tt = 0
