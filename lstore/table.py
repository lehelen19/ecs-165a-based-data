from lstore.index import Index
from lstore.index import Index
from datetime import datetime
from lstore.page import Page_Range, Page, write_to_disk

PAGE_ENTRIES = 512
PAGERANGE_ENTRIES = 512*16

INDIRECTION_COLUMN = 0 # int
RID_COLUMN = 1 # int
TIMESTAMP_COLUMN = 2 # int
SCHEMA_ENCODING_COLUMN = 3 # int

# passed in key value -> search for RID
# afer we get RID we need the record that has the RID
# record.columns[1] = '*'
# while search for record with RID of record.column[0] is not empty(or not itself)
# new record at rid .columns[1] = "*"

def get_encoding(value, index):
    return value & (1 << index)

def set_encoding(value, index):
    return value | (1 << index)

class Frame:
    """
    The collection of pages used for managing the main memory.
    Main memory pages in buffer pool are called frames: "slots" to hold a page.
    """
    def __init__(self, page_path, table_name):
        self.columns = [] # Depends on number of columns of given page
        self.page_path = page_path
        self.table_name = table_name
        self.is_dirty = False
        self.pin = False # Is the current page pinned?
        self.pin_count = 0 # Number of times page currently in given frame has been requested but not released
        self.bufferpool_time = 0 # Time of page in the bufferpool
        self.tuple_key = None
    
    def set_dirty(self):
        self.is_dirty = True
        return True
    
    def set_clean(self):
        self.is_dirty = False
        return True
    
    def pin(self):
        self.pin = True
        return True
    
    def unpin(self):
        self.pin = False
        return True

class Bufferpool:
    """
    The software layer responsible for bringing pages from disk to main memory.
    Assume we have a fixed constant number of pages (100) in the bufferpool.
    """
    def __init__(self, root_path):
        self.root_path = root_path
        self.frames = []
        self.frame_count = 0
        self.frame_directory = {} # (table_name, page_range, base/tail_index, is_base_record) : frame_index
        self.num_frames = 0
        self.merge_buffer = False
    
    def add_frame(self, table_name, page_range, index, is_base_record, frame_index):
        new_frame_key = (table_name, page_range, index, is_base_record)
        self.frame_directory[new_frame_key] = frame_index
        self.frames[frame_index].tuple_key = new_frame_key

        if self.frame_count < 100 or self.merge_buffer:
            self.frame_count += 1
    
    def at_capacity(self):
        if self.frame_count == 100:
            return True
        return False
    
    def record_present(self, table_name, record_data):
        """
        Check if the record is present in the bufferpool.
        """
        is_base_record = record_data.get("is_base_record")
        page_range = record_data.get("page_range")

        if record_data.get("is_base_record"):
            index = record_data.get("base_page")
        else:
            index = record_data.get("tail_page")
        
        frame_tuple_key = (table_name, page_range, index, is_base_record)

        if frame_tuple_key in self.frame_directory:
            return True
        return False
    
    def get_frame(self, table_name, record_data):
        is_base_record = record_data.get("is_base_record")
        page_range = record_data.get("page_range")

        if is_base_record:
            index = record_data.get("base_page")
        else:
            index = record_data.get("tail_page")
        
        frame_data = (table_name, page_range, index, is_base_record)
        return self.frame_directory.get(frame_data)
    
    def evict_page(self):
        """
        Evict the least recently used page in the bufferpool.
        If the page is dirty, then write it to the disk before eviction.
        """
        last_used_page = self.frames[0]
        frame_index = 0

        for index, frame in enumerate(self.frames):
            if frame.bufferpool_time < last_used_page.bufferpool_time:
                last_used_page = frame
                frame_index = index
        
        if last_used_page.is_dirty:
            write_path = last_used_page.page_path
            columns = last_used_page.columns
            write_to_disk(write_path, columns)
        
        frame_key = last_used_page.tuple_key
        del self.frame_directory[frame_key]

        return frame_index
    
    def load_page(self, table_name, num_columns, pr_index, index, is_base_record):
        if is_base_record:
            page_path = f"{self.path_to_root}/{table_name}/page_range_{pr_index}/" \
                        f"base_page_{index}.bin"
        else:
            page_path = f"{self.path_to_root}/{table_name}/page_range_{pr_index}/" \
                        f"tail_pages/tail_page_{index}.bin"
        
        if self.at_capacity() and not self.merge_buffer:
            frame_index = self.evict_page()
            self.frames[frame_index] = Frame(page_path=page_path, table_name=table_name)
        else:
            frame_index = self.frame_count
            self.frames.append(Frame(page_path=page_path, table_name=table_name))

        self.frames[frame_index].pin_frame()
        self.frames[frame_index].bufferpool_time = datetime.now()
        self.frames[frame_index].columns = [Page(numColumns=i) for i in range(num_columns + 5)]

        # Read from disk
        for i in range(num_columns + 5):
            self.frames[frame_index].columns[i].read_from_disk(page_path=page_path, column=i)
        
        self.add_frame(table_name, pr_index, index, is_base_record, frame_index)
        return frame_index

    def reload_page(self, frame_index, num_columns):
        frame = self.frames[frame_index]
        page_path = frame.page_path
        for i in range(num_columns + 5):
            frame.columns[i].read_from_disk(page_path=page_path, column=i)
    
    def commit_page(self, frame_index):
        frame = self.frames[frame_index]
        columns = frame.columns
        page_path = frame.page_path
        if frame.is_dirty:
            write_to_disk(page_path, columns)
            return True
        return False
    
    def commit_all(self):
        for i in range(len(self.frames)):
            if self.frames[i].is_dirty:
                self.commit_page(frame_index=i)

class Record:

    def __init__(self, rid, key, user_data, schema_encoding):
        self.rid = rid
        self.key = key
        timestamp = int(datetime.now().strftime("%d%m%Y%H%M%S"))
        self.user_data = user_data
        self.meta_data = [rid, key, timestamp, schema_encoding]
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
    def _init_(self, name, key, num_columns, bufferpool, path)
        self.name = name
        self.key = key
        self.num_columns = num_columns
        self.total_columns = 4 + num_columns
        self.page_directory = {}
        self.index = Index(self)
        self.num_records = 0
        self.page_ranges = [Page_Range(num_columns=num_columns, parent=key, pr_index=0)]
        self.bufferpool = bufferpool
        self.path = path 

    # def rid_to_location(self, rid):
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
            for base_page in page_range.pages:
                for i in range(PAGE_ENTRIES):
                    entry = base_page.pages[4].read(i)
                    if entry == key:
                        rid = base_page.pages[RID_COLUMN].read(i)
                        if rid == "*":
                            return None
                        else:
                            return rid
        return None

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
            self.page_ranges[page_range].pages[basePage].pages[i].write(value, pageIndex) # <- need to check
        return True

    def get_encoding(value, index):
        return value & (1 << index)

    def set_encoding(value, index):
        return value | (1 << index)

    def read_record(self, rid):
        page_range = self.page_directory[rid].get("page_range")
        basePage = self.page_directory[rid].get("base_page")
        pageIndex = self.page_directory[rid].get("page_index")
        print("pageindex", pageIndex)
        print("pagerange", page_range)
        print("basePage", basePage)
        entries = []

        indirectionTID = self.page_ranges[page_range].pages[basePage].pages[INDIRECTION_COLUMN].read(pageIndex)
        print("indtid", indirectionTID)
        # print("pre-for")
        for i in range(self.total_columns):
            entry = self.page_ranges[page_range].pages[basePage].pages[i].read(pageIndex)
            entries.append(entry)
        # print(entries,"entries")
        key = entries[4]
        schema_encode = entries[SCHEMA_ENCODING_COLUMN]
        # print(schema_encode, "schema_encode")
        columns = entries[4:]

        #print("schema_encode", schema_encode)
        if not schema_encode:
            #print("no change")
            return Record(rid=rid, key=key, user_data = columns, schema_encoding=schema_encode)
        else:
            print("Locating indirection")
            indLoc = self.page_ranges[page_range].pages[basePage].tailDirectory.get(indirectionTID)
            tailPage = indLoc["tail_index"]
            tailIndex = indLoc["page_index"]
            print(tailIndex)
            colUpdates = []
            for i in range(4, self.total_columns):
                if get_encoding(schema_encode, i - 4):
                    colUpdates.append(i)

            for i in colUpdates:
                # print("page_range", page_range,"basePage", basePage, "tailPage", tailPage, "i", i, "page", pageIndex)
                columns[i - 4] = self.page_ranges[page_range].pages[basePage].tail[tailPage].pages[i].read(tailIndex)
            print(f"user cols: {columns}")
        return Record(rid=rid, key=key, user_data=columns, schema_encoding=schema_encode)


        # if "1" not in schema_encode:
        #     return Record(key= key, rid = rid, user_data = columns, schema_encoding = schema_encode)
        # else:
        #     indDict = self.page_ranges[page_range].base_pages[basePage].tailDirectory.get(indTID)
        #     tailPage = indDict.get("tail_page")
        #     tailIndex = indDict.get("page_index")
        #     updatedCols = []

        #     for i in range(4, self.total_columns):
        #         if get_bit(schema_encode, i-4):
        #             updatedCols.append(i)

        #     for i in updatedCols:
        #         columns[i-4] = self.page_ranges[page_range].base_pages[basePage].tail[tailPage].pages[i].read(tailIndex)

        # return Record(rid=rid, key=key, user_data=columns, schema_encoding=schema_encode)


    def update_record(self, rid, record):
        #print("Starting to update record")
        record_info = self.page_directory.get(rid)
        pageRange = record_info.get("page_range")
        basePage = record_info.get("base_page")
        pageIndex = record_info.get("page_index")

        prevTID = self.page_ranges[pageRange].pages[basePage].pages[INDIRECTION_COLUMN].read(pageIndex)
        newTID = self.page_ranges[pageRange].pages[basePage].createTID(num_columns=self.num_columns)
        print("newtid", newTID)
        newTIDLocation = self.page_ranges[pageRange].pages[basePage].tailDirectory.get(newTID)
        print(newTIDLocation, "location dictionary")

        newTail = newTIDLocation.get("tail_index")
        newPage = newTIDLocation.get("page_index")

        record.columns[INDIRECTION_COLUMN] = prevTID
        record.columns[RID_COLUMN] = newTID
        print("Indirection", record.columns[0])

        for i in range(len(record.columns)):
            val = record.columns[i]
            #print(f"Value is {val}")
            self.page_ranges[pageRange].pages[basePage].tail[newTail].pages[i].write(val, newPage)

        newSchema = record.columns[SCHEMA_ENCODING_COLUMN]
        updateInd = self.page_ranges[pageRange].pages[basePage].pages[INDIRECTION_COLUMN].write(newTID, newPage)
        #print(f"Updated index: {updateInd}")
        updateSchema = self.page_ranges[pageRange].pages[basePage].pages[SCHEMA_ENCODING_COLUMN].write(newSchema, newPage)
        #print(f"Wrote schema: {updateSchema}")
        #print(f'read updated_schema = {self.page_ranges[pageRange].pages[basePage].pages[SCHEMA_ENCODING_COLUMN].read(newPage)}')
        return True


    def add_page_range(self):
        range_index =  len(self.page_ranges)
        self.page_ranges.append(Page_Range(num_columns=self.num_columns, parent=self.key, pr_index=range_index))

    def __merge(self):
        print("merge is happening")
        pass
