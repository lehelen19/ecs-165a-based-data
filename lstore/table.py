from sqlalchemy import column
from lstore.index import Index
from datetime import datetime

from lstore.page import Page_Range

INDIRECTION_COLUMN = 0 # int
RID_COLUMN = 1 # int
TIMESTAMP_COLUMN = 2 # int
SCHEMA_ENCODING_COLUMN = 3 # string

# passed in key value -> search for RID
# afer we get RID we need the record that has the RID
# record.columns[1] = '*'
# while search for record with RID of record.column[0] is not empty(or not itself)
# new record at rid .columns[1] = "*"

class Record:

    def __init__(self, rid, key, user_data, schema_encoding):
        self.rid = rid
        self.key = key
        timestamp = int(datetime.now().strftime("%d%m%Y%H%M%S"))
        self.user_data = user_data
        self.meta_data = [0, rid, timestamp, schema_encoding]
        self.columns = self.meta_data + self.user_data

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
        self.key = key
        self.num_columns = num_columns
        self.total_columns = 4 + num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.num_records = 0
        self.page_ranges = [Page_Range(index=0, Table=self)]

    def create_rid(self):
        """
        Create a new RID for a given record based on
        number of records in the table and
        adds the RID-record mapping to the page directory.
        """
        rid = self.num_records
        self.num_records += 1
        # self.page_directory[rid] = {"page_range": page_range_index,
        #                             "column": column_index,
        #                             "page": page_index}
        # rid -> page_range -> column -> page
        # look at all columns? and retrieve multiple pages
        return rid

    #return the rid of the record given key
    def key_get_RID(self, key):
        for record in records:
            if record[4] == key:
                return(record(1))

    def write_record(self, rid, record):
        page_range = self.page_directory[rid].get("page_range") # page range index
        # Writes record to the location based on RID
        for i in range(len(record.columns)):
            value = record.columns[i]
            # value = [0, rid, timestamp, schema_encoding, 01209, 124908, 129058...]
            self.page_ranges[page_range].columns[i].curr_page.write(value) # <- need to check
        return True
    
    def read_record(self, rid):
        """
        Returns record information based on RID.
        """
        page_range = self.page_directory[rid].get("page_range")
        # next_page = self.page_ranges[page_range].columns[0].pages.read() # EDIT EDIT EDIT
        record = []
        return record

    def update_record(self, rid, record):
        pass

    def add_page_range(self, index):
        self.page_ranges.append(Page_Range(index, self))

    def __merge(self):
        print("merge is happening")
        pass
