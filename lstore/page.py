class BasePage:

    def __init__(self, index, total_columns, pageRangeIndex):
        self.pages = [Page(index=i, _type="base") for i in range(total_columns)] #eq to colmuns_list
        self.tail = [TailPage(index = 0, parent = index, total_columns = total_columns)] # eq to tail_page_list
        self.tailDirectory = {} # eq to tail_page_directectory
        self.pageRangeIndex = pageRangeIndex # eq to to pr_key
        self.index = index # index of column where 0 = indirection, etc., eq to bp key
        self.numTailRecords = 0

    def newTailPage(self):
        tailIndex = len(self.tail)
        newTail = TailPage(tailIndex, self.index)
        self.tail.append(newTail)

    def createTID(self):
        TID = self.numTailRecords
        self.numTailRecords += 1
        tailIndex = TID // 512
        if tailIndex > len(self.tail)-1:
            self.newTailPage()
        self.tailDirectory[TID] = {"tail_index": tailIndex, "page_index" : TID % 512}
        return TID

class TailPage:

    def __init__(self, index, parent, total_columns):
        self.pages = [Page(index=i, _type="tail") for i in range(total_columns)] # eq to columns_list
        self.index = index # index of column where 0 = indirection, etc., eq to key
        self.parent = parent  #eq to bp_key

class Page:

    """
    :param num_records: int     #Numer of records in page
    :param data: bytearray      #4KB bytearray to hold data (int -> bytes)
    :param index: int           #Index (page number) of page in column
    :param column: key          #Key denoting associated column
    """

    def __init__(self, index, _type):
        self.num_records = 0
        self.data = bytearray(4096)
        self.index = index # based on number of columns
        self.type = _type

    def has_capacity(self):
        if self.num_records >=512:
            return False
        return True


    def write(self, value, location):
        start = location * 512
        if type(value) == int:
            self.data[start:start+8] = value.to_bytes(8, byteorder="big")
        elif type(value) == str:
            value = int(value)
            print("Value", value)
            self.data[start:start+8] = value.to_bytes(8, byteorder="big")
            print(self.data[start:start+8], "self.data[start:start+8]")
        self.num_records += 1
        return True


    def read(self, location, rtype):
        if rtype == str:
            temp = int.from_bytes(bytes = self.data[location*512: location*512+8], byteorder = "big")
            print(str(temp))
            return str(temp)
        return int.from_bytes(bytes = self.data[location*512: location*512+8], byteorder = "big")

class Page_Range:

    def __init__(self, index, Table):
        """
        Maintains the columns of the table for the physical pages.
        This also keeps track of how the base pages are grouped, for example.
        We'll need the number of columns and the primary key column.
        """

        self.tail_pages = []
        self.index = index # identifier in table
        self.num_columns = Table.num_columns
        self.total_columns = Table.total_columns
        self.base_pages = [BasePage(index=0, total_columns=self.total_columns, pageRangeIndex = index)]
        self.table_key = Table.key

# class Column:

#     def __init__(self, index):
#         self.pages = [Page(index=0, _type="base")]
#         self.index = index # index of column where 0 = indirection, etc.
#         self.curr_page = self.pages[0]

#     def add_page(self, index, _type):
#         self.pages.append(Page(index, _type))
#         self.curr_page = self.pages[index]
#         self.curr_page = self.pages[index]
