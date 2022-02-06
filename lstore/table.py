from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3


class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
<<<<<<< HEAD
    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {}
        self.index = Index(self)
        pass

    def __merge(self):
        print("merge is happening")
        pass
 
=======
    def __init__(self, name, num_columns, key, db):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.page_directory = {} #dictionary of page ranges and their corresponding pages
        self.total_columns = 4 + num_columns
        self.index = Index(self)
        db.tables.append(self)
        pass


    def __merge(self):
        print("merge is happening")
        pass
>>>>>>> bbea90a0f46c0492bd99cd5b80b8a368463be3fd
