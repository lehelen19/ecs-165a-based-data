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
<<<<<<< Updated upstream
        self.columns = columns
=======
        timestamp = int(datetime.now().strftime("%d%m%Y%H%M%S"))
        self.user_data = user_data
        self.meta_data = [rid, rid, timestamp, schema_encoding]
        self.columns = self.meta_data + self.user_data
>>>>>>> Stashed changes


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
<<<<<<< Updated upstream
        db.tables.append(self)

=======
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



    #return the rid of the record given key

    # Invalid syntax
    # def key_get_RID(self, key):
    #     for i in range(len(record.columns[4])):
    #         if record.columns[4][i]= key:
    #             return record.columns[1][i]


    def write_record(self, rid, record):
        # print()
        # print("writing record")
        # print()
        # print("self.page_directory[rid]", self.page_directory[rid])
        page_range = self.page_directory[rid].get("page_range") # page range index
        print(page_range, "page range")
        print(record.columns, "record col")
        # Writes record to the location based on RID
        for i in range(len(record.columns)):
            value = record.columns[i]
            # value = [0, rid, timestamp, schema_encoding, 01209, 124908, 129058...]
            print(self.page_ranges[page_range].columns[i].curr_page.write(value)) # <- need to check

        return True 


    # def write_record(self, rid, record):
    #     page_range = self.page_directory[rid].get("page_range") # page range index
    #     # Writes record to the location based on RID
    #     for i in range(len(record.columns)):
    #         value = record.columns[i]
    #         # value = [0, rid, timestamp, schema_encoding, 01209, 124908, 129058...]
    #         self.page_ranges[page_range].columns[i].curr_page.write(value) # <- need to check
    #     return True
    
    def read_record(self, rid):
        """
        Returns record information based on RID.
        """
        pass

    def update_record(self, rid, record):
        pass

    def add_page_range(self):
        pass
>>>>>>> Stashed changes

    def __merge(self):
        print("merge is happening")
        pass

