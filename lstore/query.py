from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page, Page_Range, BasePage, TailPage
# def get_encoding(value, index):
#     return value & (1 << index)

# def set_encoding(value, index):
#     return value | (1 << index)

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        self.tailRID = 64001
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        rid = table.key_get_RID(primary_key)
        if rid is None:
            return False
        record = self.table.read_record(rid)
        print(record)
        if record.column[0] == record.rid:
            record.columns[1] = '*'
        elif (record.columns[1] != record.columns[0]): #if there are any updates check
            tail_record = self.table.read_record(record.columns[0])
            while tail_record.columns[1] != tail_record.columns[0]:
                tail_record.columns[1] = '*'
                tail_record = self.table.read_record(tail_record.columns[0])
            tail_record.columns[1] = '*'
        return True






    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *cols):
        list_columns = list(cols)
        
        if len(list_columns) != self.table.num_columns:
            return False

        self.rid = self.table.createRid()
        new_record = Record(key = cols[0], rid = self.rid, user_data = list_columns, schema_encoding = 0)
        self.table.write_record(self.rid, new_record)

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """

    def select(self, index_value, index_column, query_columns):
        if len(query_columns) != self.table.num_columns or index_column > self.table.num_columns or index_column < 0:
            print("error")
            return False
        # error checking
        for value in query_columns:
            if value !=0 and value != 1:
                print("errors")
                return False

        rid = self.table.key_get_RID(index_value)
        print("RIIIDDIDIDID", rid)
        if rid is None:
            return False
        record = self.table.read_record(rid)
        print("recccccc", record)
        if record == False:
            return False
        records = []
        
        for index in range(len(query_columns)):
            if query_columns[index] == 1:
                continue
            else:
                record.user_data[index] = None
        records.append(record)
        return records


    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """
    # flawed
    def update(self, primary_key, *columns):
        columns = columns[0]
        # list_columns = list(columns)
        print("lil col", columns)
        # if len(list_columns) != table.num_columns:
        #     return False
        # self.rid = tailRID

        # rid = key_get_RID(columns(0))
        rid = self.table.key_get_RID(primary_key) # base rid
        print("rid", rid)
        record = self.table.read_record(rid)
        print("record",record)

        # self.table.update_record(rid, record)

        # record.columns[0] = tailRID # need to set a new rid 

        # need to check where ths 
        #  create schema encoding
        schema_encoding = record.columns[3]
        # print(schema_encoding, "schema_encoding")
        print("pre-loop")
        print("enc", record.columns[3], type(record.columns[3]))
        for i in range(len(columns)):
            if record.columns[i] == None and not self.table.get_encoding(value = record.columns[3], index = i):
                record.user_data[i] = 0
            else:
                print("enc", record.columns[3], type(record.columns[3]))
                schema_encoding = Table.set_encoding(schema_encoding, i)
                record.user_data[i] = columns[i]
        #  create new record with updated data, still need to get the RID sorted out 
        new_record = Record(key = columns[0], rid = rid, schema_encoding = schema_encoding, user_data = columns)
        return self.table.update_record(rid, new_record)

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        # read from base pages, error checking
        if start_range < 0 or end_range < 0:
            return False
        if aggregate_column_index < 0 or aggregate_column_index > self.table.num_columns:
            return False

        column_sum = 0
        founded_key = False
        for pagerange in self.table.page_ranges:
            for basepages in pagerange.pages:
                for i in range(512):
                    key=basepages.pages[4].read(i)
                    if key >= start_range and key <= end_range:
                        rid = basepages.pages[1].read(i)
                        record = self.table.read_record(rid)
                        user_columns = record.user_data
                        column_sum += pages[aggregate_column_index]
                        founded_key = True
        
        if not founded_key:
            return False

        return column_sum

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """
    def increment(self, key, column):
        r = self.select(key, self.table.key, [1]*self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
