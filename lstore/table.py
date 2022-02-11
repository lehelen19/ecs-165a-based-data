from lstore.index import Index
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
        for i in range(len(record.columns[4])):
            if record.columns[4][i]= key:
                return record.columns[1][i]


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

        record_info = self.page_directory.get(rid)
        pageRange = record_info.get("page_range")
        basePage = record_info.get("base_page")
        pageIndex = record_info.get("page_index")

        total_entries = []

        indirection_tid = self.book[pageRange].pages[basePage].columns_list[INDIRECTION_COLUMN].read(pageIndex)

        for col in range(self.total_columns):
            entry = self.book[pageRange].pages[basePage].columns_list[col].read(pageIndex)
            total_entries.append(entry)
        key = total_entries[columns[0]]
        schema_encode = total_entries[SCHEMA_ENCODING_COLUMN]
        user_cols = total_entries[columns[0]: ]
        if not schema_encode:
            return Record(key= key, rid = rid, schema_encoding = schema_encode, column_values = user_cols)
        else:
            ind_dict = self.book[pageRange].pages[basePage].tail_page_directory.get(indirection_tid)
            tail_page = ind_dict.get('tail_page')
            tp_index = ind_dict.get('page_index')
            column_update_indices = []
            for i in range(columns[0], self.total_columns):
                if get_bit(schema_encode, i - 4)
                    column_update_indices.append(i)
            for index in column_update_indices:
                user_cols[index - 4] = self.book[pageRange].pages[basePage].tail_page_list[tail_page].columns_list[index].read(tp_index)

        return Record(key= key, rid = rid, schema_encoding = schema_encode, column_values = user_cols)

        # page_range = self.page_directory[rid].get("page_range")
        # # next_page = self.page_ranges[page_range].columns[0].pages.read() # EDIT EDIT EDIT
        # record = []
        # for i in range(len(record.columns[1])):
        #     if record.columns[1][i]= rid:
        #         for j in range(len(record.columns)):
        #             record.append(record.columns[j][i])
        # return record

    def update_record(self, rid, record):
        pass

    def add_page_range(self):
        pass

    def __merge(self):
        print("merge is happening")
        pass
