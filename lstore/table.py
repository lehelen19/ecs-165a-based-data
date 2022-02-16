from sqlalchemy import column
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
        self.meta_data = [rid, rid, timestamp, schema_encoding]
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

    def rid_to_location(self, rid: int) -> dict:

        page_entries = int(4096 / 8)
        pageRange_entries = page_entries * 16

        range_index = (rid // pageRange_entries)
        index = rid % pageRange_entries
        base_index = index // page_entries
        phy_index = index % page_entries

        page_directory = {
            'page_range': range_index,
            'base_page': base_index,
            'page_index': phy_index
        }

        return page_directory


    def create_rid(self):
        # print("create RID ")
        """
        Create a new RID for a given record based on
        number of records in the table and
        adds the RID-record mapping to the page directory.
        """
        rid = self.num_records
        self.num_records += 1
        self.page_directory[rid] = self.rid_to_location(rid)

        # rid -> page_range -> column -> page
        # look at all columns? and retrieve multiple pages
        # print("rid", rid)
        return rid




    def key_get_RID(self, key):
        pass

    #     for page in self.page_range:
    #         for p in page.pages:


        # search though the entire database and ge thte RID from the key
        # itterate thorugh all page ranges 
            # iterate through all base pages
                # check if the value in the key column is the same as the desired key, if yes return the RID of that column 

        # for i in range(len(record.columns[4])):
        #     if record.columns[4][i] == key:
        #         return record.columns[1][i]


    def write_record(self, rid, record):
        page_range = self.page_directory[rid].get("page_range") # page range index
        print(page_range, "page range")
        print(record.columns, "record col")
        for i in range(len(record.columns)):
            value = record.columns[i]
            print(self.page_ranges[page_range].columns[i].curr_page.write(value)) # <- need to check
        return True 

    
    def read_record(self, rid):
        page_range = self.page_directory[rid].get("page_range")
        basePage = self.page_directory[rid].get("base_page")
        pageIndex = self.page_directory[rid].get("page_index")
        entries = []

        for i in range(self.total_columns):
            rec = self.page_ranges[page_range].pages[basePage].columns_list[i].read(pageIndex)
            entries.append(rec)

        key = entries[4]
        schema_encode = entries[SCHEMA_ENCODING_COLUMN]
        columns = entries[self.columns]

        return Record(key= key, rid = rid, schema_encoding = schema_encode, column_values = columns)


    def update_record(self, rid, record):
        pass


    def add_page_range(self, index):
        self.page_ranges.append(Page_Range(index, self))

    def __merge(self):
        print("merge is happening")
        pass

