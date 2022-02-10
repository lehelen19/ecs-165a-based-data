from lstore.index import Index
from time import time

INDIRECTION_COLUMN = 0
RID_COLUMN = 1
TIMESTAMP_COLUMN = 2
SCHEMA_ENCODING_COLUMN = 3

# passed in key value -> search for RID
# afer we get RID we need the record that has the RID
# record.columns[1] = '*'
# while search for record with RID of record.column[0] is not empty(or not itself)
# new record at rid .columns[1] = "*"




class Record:

    def __init__(self, rid, key, columns):
        self.rid = rid
        self.key = key
        self.columns = columns
        self.schema_encoding = schema_encoding
        self.records = []
        self.tailrecords = []

class Table:

    """
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """

    def __init__(self, name, num_columns, key, db):
        self.name = name
        self.key = key # indicates which column is the primary key
        self.num_columns = num_columns
        self.page_directory = {} #dictionary of page ranges and their corresponding pages
        self.total_columns = 4 + num_columns
        self.index = Index(self)
        #self.baseRID = baseRID
        db.tables.append(self)

#return the rid of the record given key
    def key_get_RID(self, key):
        for record in records:
            if record[4] == key:
                return(record(1))

    def read_record(self,rid):
        pass



    def __merge(self):
        print("merge is happening")
        pass
