
class Page:

    def __init__(self, index, column):
        self.num_records = 0
        self.data = bytearray(4096)
        self.index = index # based on number of columns
        self.column = column
        column.pages.append(self)
        self.next = 0

    def has_capacity(self):
        if self.num_records >=512:
            return False
        return True

    def new_page(self):
        pass
    def write(self, value):
        self.next += 8
        self.num_records += 1
        self.data[self.next] = value.to_bytes(8, "big")

class Page_Range:

    def __init__(self,index,Table):
        """
        Maintains the columns of the table for the physical pages.
        This also keeps track of how the base pages are grouped, for example.
        We'll need the number of columns and the primary key column.
        """
        self.columns = []
        self.index = index
        self.total_columns = Table.total_columns
        # Iterate through columns to get pages
        # Table.page_directory[self] = self.columns

        for i in range(self.total_columns):
            column = Column
            column.index = i
            self.columns.append(column)


class Column:

    def __init__(self, index):
        self.pages = []
        self.index = index
