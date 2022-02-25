"""
A data strucutre holding indices for various columns of a table. Key column should be indexd by default, other columns can be indexed through this object. Indices are usually B-Trees, but other data structures can be used as well.
"""
PAGE_ENTRIES = 512
PAGERANGE_ENTRIES = 512*16

INDIRECTION_COLUMN = 0 # int
RID_COLUMN = 1 # int
TIMESTAMP_COLUMN = 2 # int
SCHEMA_ENCODING_COLUMN = 3 # int
class Index:

    def __init__(self, table):
        # One index for each table. All our empty initially.
        self.indices = [None] *  table.num_columns
        self.table = table
        pass

    """
    # returns the location of all records with the given value on column "column"
    """

    def locate(self, column, value):
        pass

    """
    # Returns the RIDs of all records with values in column "column" between "begin" and "end"
    """

    def locate_range(self, begin, end, column):
        pass

    """
    # optional: Create index on specific column
    """

    def create_index(self, column_number):

        pass

    """
    # optional: Drop index of specific column
    """

    def drop_index(self, column_number):
        if column_number == 0:
            return False
        self.indices[column_number] = None

class NewIndex:
    def __init__(self, table, column) -> None:
        self.index = {}
        
        total_rid = self.num_records - 1
        total_page_range = total_rid / PAGERANGE_ENTRIES

