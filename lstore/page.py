class BasePage:

    def __init__(self, num_columns, parent, index):
        self.pages = [Page(numColumns=i) for i in range(num_columns + 4)] # list of physical pages
        self.tail = [TailPage(num_columns=num_columns, index=0, parent=index)] # list of tail pages
        self.tailDirectory = {} # directory mapping tail page to tail index and page index
        self.range_index = parent # index of parent page_range
        self.index = index
        self.numTailRecords = 0
        self.numTailPages = 1


    def newTailPage(self, num_columns):
        tailIndex = len(self.tail)
        newTail = TailPage(num_columns=num_columns, parent=self.key, index=tailIndex)
        self.tail.append(newTail)
        return f"Created new tail page. Tail index: {tailIndex}"

    def createTID(self, num_columns):
        TID = self.numTailRecords
        self.numTailRecords += 1
        
        tailIndex = TID // 512
        if tailIndex > len(self.tail)-1:
            self.newTailPage(num_columns)
        self.tailDirectory[TID] = {"tail_index": tailIndex, "page_index" : TID % 512}
        return TID

    def TID_to_location(self, TID):
        tailIndex = TID // 512
        pageIndex = TID % 512
        return{"tail_page": tailIndex, "page_index": pageIndex}

class TailPage:

    def __init__(self, num_columns, parent, index):
        self.pages = [Page(numColumns=i) for i in range(num_columns + 4)] # eq to columns_list
        self.index = index # index of column where 0 = indirection, etc., eq to key
        self.parent = parent  #eq to bp_key

class Page:

    """
    :param num_records: int     #Numer of records in page
    :param data: bytearray      #4KB bytearray to hold data (int -> bytes)
    :param index: int           #Index (page number) of page in column
    :param column: key          #Key denoting associated column
    """

    def __init__(self, numColumns):
        self.num_records = 0
        self.data = bytearray(4096)
        self.numColumns = numColumns

    def has_capacity(self):
        if self.num_records >= 512:
            return False
        return True


    def write(self, value, location):
        if location > 512:
            return False
        start = location * 8
        self.data[start : (start + 8)] = value.to_bytes(8, "big")
        self.num_records += 1
        # if type(value) == int:
        #     self.data[start:start+8] = value.to_bytes(8, byteorder="big")
        # elif type(value) == str:
        #     value = int(value)
        #     print("Value", value)
        #     self.data[start:start+8] = value.to_bytes(8, byteorder="big")
        #     print(self.data[start:start+8], "self.data[start:start+8]")
        # self.num_records += 1
        return True


    def read(self, location):
        start = location * 8
        res = self.data[start : (start + 8)]
        ret_val = int.from_bytes(bytes=res, byteorder="big")
        return ret_val
        # if rtype == str:
        #     temp = int.from_bytes(bytes = self.data[location*512: location*512+8], byteorder = "big")
        #     print(str(temp))
        #     return str(temp)
        # return int.from_bytes(bytes = self.data[location*512: location*512+8], byteorder = "big")
    
    def read_from_disk(self, page_path, column):
        _file = open(page_path, "rb").seek(column * 4096)
        self.data = bytearray(_file.read(4096))
        _file.close()
        return True

def write_to_disk(page_path, columns):
    _file = open(page_path, "wb")
    for i in range(len(columns)):
        _file.write(columns[i].data)
    _file.close()

class Page_Range:

    def __init__(self, num_columns, parent, pr_index):
        """
        Maintains the columns of the table for the physical pages.
        This also keeps track of how the base pages are grouped, for example.
        We'll need the number of columns and the primary key column.
        """
        self.index = pr_index # identifier in table
        self.num_columns = num_columns
        self.pages = [BasePage(num_columns=num_columns, parent=pr_index, index=i) for i in range(16)]
        self.table_key = parent

# class Column:

#     def __init__(self, index):
#         self.pages = [Page(index=0, _type="base")]
#         self.index = index # index of column where 0 = indirection, etc.
#         self.curr_page = self.pages[0]

#     def add_page(self, index, _type):
#         self.pages.append(Page(index, _type))
#         self.curr_page = self.pages[index]
#         self.curr_page = self.pages[index]
