from lstore.index import Index
from lstore.index import Index
from datetime import datetime
from lstore.page import Page_Range

PAGE_ENTRIES = 512
PAGERANGE_ENTRIES = 512*16

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

    def __init__(self, name, num_columns, key):
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.total_columns = 4 + num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.num_records = 0
        self.page_ranges = [Page_Range(num_columns=num_columns, parent=key, pr_index=0)]

    # def rid_to_location(self, rid):
    #     range_index = (rid // PAGERANGE_ENTRIES)
    #     index = rid % PAGERANGE_ENTRIES
    #     base_index = index // PAGE_ENTRIES
    #     phy_index = index % PAGE_ENTRIES

    #     return {
    #         'page_range': range_index,
    #         'base_page': base_index,
    #         'page_index': phy_index
    #     }

    def createRid(self):
        """
        Create a new RID for a given record based on
        number of records in the table and
        adds the RID-record mapping to the page directory.
        """
        rid = self.num_records
        self.num_records += 1

        range_index = (rid // PAGERANGE_ENTRIES)
        index = rid % PAGERANGE_ENTRIES
        base_index = index // PAGE_ENTRIES
        phy_index = index % PAGE_ENTRIES

        if range_index > len(self.page_ranges)-1:
            self.add_page_range()

        page_directory = {
            'page_range': range_index,
            'base_page': base_index,
            'page_index': phy_index
        }

        self.page_directory[rid] = page_directory

        return rid

    def key_get_RID(self, key):
        for page_range in self.page_ranges:
            for base_page in page_range.base_pages:
                for i in range(PAGE_ENTRIES):
                    entry = base_page.pages[4].read(i, int)
                    if entry == key:
                        rid = base_page.pages[RID_COLUMN].read(i, int)
                        if rid == "*":
                            return False
                        else:
                            return rid
        return False

    def check_tail_capacity(self, base_page):
        tailPage = 0
        for page in base_page.tail:
            for i in range(page.pages[INDIRECTION_COLUMN]):
                if page.pages[INDIRECTION_COLUMN].read[i] == 0:
                    return {"tailPage": tailPage, "tailIndex": i}
        tailPage += 1

        pass

    def check_base_capacity(self):
        pass


    def write_record(self, rid, record):
        page_range = self.page_directory[rid].get("page_range") # page range index
        basePage = self.page_directory[rid].get("base_page")
        pageIndex = self.page_directory[rid].get("page_index")

        for i in range(len(record.columns)):
            value = record.columns[i]
            print(self.page_ranges[page_range].pages[basePage].pages[i].write(value, pageIndex)) # <- need to check
        return True

    def read_record(self, rid): # TODO: EDIT
        page_range = self.page_directory[rid].get("page_range")
        basePage = self.page_directory[rid].get("base_page")
        pageIndex = self.page_directory[rid].get("page_index")
        entries = []

        indirectionTID = self.page_ranges[page_range].pages[basePage].pages[INDIRECTION_COLUMN].read(pageIndex)

        print("pre-for")
        for i in range(self.total_columns):
            entry = self.page_ranges(page_range).pages[basePage].pages[i].read(pageIndex)
            entries.append(entry)
        print(entries,"entries")
        key = entries[4]
        schema_encode = entries[SCHEMA_ENCODING_COLUMN]
        print(schema_encode, "schema_encode")
        columns = entries[4:]

        if not schema_encode:
            return Record(rid=rid, key=key, columns=columns, schema_encoding=schema_encode)

        if "1" not in schema_encode:
            return Record(key= key, rid = rid, user_data = columns, schema_encoding = schema_encode)
        else:
            indDict = self.page_ranges[page_range].base_pages[basePage].tailDirectory.get(indTID)
            tailPage = indDict.get("tail_page")
            tailIndex = indDict.get("page_index")
            updatedCols = []

            for i in range(4, self.total_columns):
                if get_bit(schema_encode, i-4):
                    updatedCols.append(i)

            for i in updatedCols:
                columns[i-4] = self.page_ranges[page_range].base_pages[basePage].tail[tailPage].pages[i].read(tailIndex)

        return Record(rid=rid, key=key, user_data=columns, schema_encoding=schema_encode)


    def update_record(self, rid, record):
        record_info = self.page_directory.get(rid)
        pageRange = record_info.get("page_range")
        basePage = record_info.get("base_page")
        pageIndex = record_info.get("page_index")

        prevTID = self.page_ranges[pageRange].base_pages[basePage].pages[INDIRECTION_COLUMN].read(pageIndex)
        newTID = self.page_ranges[pageRange].base_pages[basePage].createTID()
        newTIDLocation = self.page_ranges[pageRange].base_pages[basePage].tail_page_directory.get(newTID)

        newTail = newTIDLocation.get("tail_index")
        newPage = newTIDLocation.get("page_index")

        record.columns[INDIRECTION_COLUMN] = prevTID
        record.columns[RID_COLUMN] = newTID

        for i in range(len(record.columns)):
            val = record.columns[i]
            self.page_ranges[pageRange].base_pages[basePage].tail[newTail].pages[i].write(column_values, newPage)

        newSchema = record.columns[SCHEMA_ENCODING_COLUMN]
        updateInd = self.page_ranges[pageRange].base_pages[basePage].pages[INDIRECTION_COLUMN].write(newTID, newPage)
        updateSchema = self.page_ranges[pageRange].base_pages[basePage].pages[SCHEMA_ENCODING_COLUMN].write(newSchema, newPage)



    def add_page_range(self, index):
        range_index =  len(self.page_ranges)
        self.page_ranges.append(Page_Range(num_columns=self.num_columns, parent=self.key, pr_index=index))

    def __merge(self):
        print("merge is happening")
        pass
