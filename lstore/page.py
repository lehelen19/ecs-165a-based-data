
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
