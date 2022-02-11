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
        self.next = 0
        self.type = _type

    def has_capacity(self):
        if self.num_records >=512:
            return False
        return True


    def write(self, value): # Add based on entry row?
        # print("value", value)
        # print(type(value.to_bytes(8, byteorder="big")))
        # print(type(self.data[self.next]))
        # print(type(self.next))
        if type(value) == int:
            self.data[self.next: self.next+8] = value.to_bytes(8, byteorder="big") 
        elif type(value) == str:
            self.data[self.next: self.next+8] = bytearray(value,"ascii")
        # print(self.data[self.next: self.next+8], end = ", ")
        self.next += 8
        self.num_records += 1
        return True 

    
    def read(self, location):
        # Convert bytes into int
        pass

    def read(self, location):
        # Convert bytes into int
        pass

class Page_Range:

    def __init__(self, index, Table):
        """
        Maintains the columns of the table for the physical pages.
        This also keeps track of how the base pages are grouped, for example.
        We'll need the number of columns and the primary key column.
        """
        self.columns = []
        self.index = index # identifier in table
        self.num_columns = Table.num_columns
        self.total_columns = Table.total_columns
        self.table_key = Table.key

        # Iterate through columns to get pages
        # Table.page_directory[self] = self.columns
        for i in range(self.total_columns):
            column = Column(i)
            self.columns.append(column)


class Column:

    def __init__(self, index):
        self.pages = [Page(index=0, _type="base")]
        self.index = index # index of column where 0 = indirection, etc.
        self.curr_page = self.pages[0]

    def add_page(self, index, _type):
        self.pages.append(Page(index, _type))
        self.curr_page = self.pages[index]
        self.curr_page = self.pages[index]
