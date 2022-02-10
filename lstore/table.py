from lstore.index import Index
from time import time

from lstore.page import Page_Range

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
        #self.schema_encoding = schema_encoding
        self.records = []
        self.tailrecords = []

class Table:

    """
    :param name: string         #Table name
    :param key: int             #Index of table key in columns
    :param num_columns: int     #Number of user columns in the table
    :param total_columns: int   #Number of user + meta columns in table
    :param page_directory: dict #Maps RID to corresponding record's physical location
    :param index: Index         #Index object instance for table
    :param num_records: int     #Number of records in table
    :param page_ranges: list    #List of page ranges associated with table (initialized)
    """

    def __init__(self, name, num_columns, key, db):
        self.name = name
        self.key = key # key associated with table in the database
        self.num_columns = num_columns
        self.total_columns = 4 + num_columns
        self.page_directory = {} # dictionary of page ranges and their corresponding pages
        self.index = Index(self)
        self.num_records = 0
        self.page_ranges = [Page_Range(index=0, Table=self)]
        db.tables.append(self) # I don't know if we need

    def create_rid(self):
        """
        Create a new RID for a given record based on
        number of records in the table, then increments.
        """
        rid = self.num_records
        self.num_records += 1
        return rid

    #return the rid of the record given key
    def key_get_RID(self, key):
        for record in records:
            if record[4] == key:
                return(record(1))

    def read_record(self, rid):
        pass
    
    def add_page_range(self):
        pass

    def __merge(self):
        print("merge is happening")
        pass
