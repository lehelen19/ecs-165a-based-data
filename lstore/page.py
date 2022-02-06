
class Page:

    def __init__(self):
        self.num_records = 0
        self.data = bytearray(4096)

    def has_capacity(self):
        pass
    def __init__(self,index, Page_Range):
        self.num_records = 0
        self.data = bytearray(4096)
        self.index = index
        self.Page_Range= Page_Range
        Page_Range.pages.append(self)

    def has_capacity(self):
        if self.num_records >=512:
            return False
        return True
    

    def write(self, value):
        self.num_records += 1
        pass

class Page_Range:

    def __init__(self,index,Table):
        self.pages = []
        self.index = index
        Table.page_directory[self] = self.pages
