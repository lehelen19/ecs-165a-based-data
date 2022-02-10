
class Page:

    """
    :param num_records: int     #Numer of records in page
    :param data: bytearray      #4KB bytearray to hold data (int -> bytes)
    :param index: int           #Index (page number) of page in column
    :param column: key          #Key denoting associated column
    """

    def __init__(self, index):
        self.num_records = 0
        self.data = bytearray(4096)
        self.index = index # based on number of columns
        self.next = 0

    def has_capacity(self):
        if self.num_records >=512:
            return False
        return True

    def write(self, value):
        self.next += 8
        self.num_records += 1
        self.data[self.next] = value.to_bytes(8, "big")
    
    def read(self, value):
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
            # column.index = i
            self.columns.append(column)


class Column:

    def __init__(self, index):
        self.pages = [Page(index=0)]
        self.index = index # index of column where 0 = indirection, etc.
