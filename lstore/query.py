from lstore.table import Table, Record
from lstore.index import Index
from lstore.page import Page, Page_Range, BasePage, TailPage

def get_encoding(value, index):
    return value & (1 << index)

def set_encoding(value, index):
    return value | (1 << index)

class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table

    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        real_rid = self.table.key_get_RID(primary_key)
        record = self.table.read_record(rid)
        for pagerange in self.table.page_ranges:
            for basepages in pagerange.pages:
                for i in range(512):
                    rid = basepages.pages[1].read(i)
                    if rid == real_rid:
                        self.write("*", basepages.pages[1])
                        indirection = basepages.pages[0].read(i)
                        if indirection != 0:
                            real_rid = indirection
                            continue

                        else:
                            return True
        # rid = self.table.key_get_RID(primary_key)
        # if rid is None:
        #     return False
        # record = self.table.read_record(rid)
        # print(record)
        # if record.column[0] == record.rid:
        #     record.columns[1] = '*'
        # elif (record.columns[1] != record.columns[0]): #if there are any updates check
        #     tail_record = self.table.read_record(record.columns[0])
        #     while tail_record.columns[1] != tail_record.columns[0]:
        #         tail_record.columns[1] = '*'
        #         tail_record = self.table.read_record(tail_record.columns[0])
        #     tail_record.columns[1] = '*'
        # return True



    def check_values(self, values):
        for val in values:
            if val < 0:
                return False
            elif not isinstance(val, int):
                return False
            elif val == None:
                return False
        return True


    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *cols):
        print("Insert starting!")
        list_columns = list(cols)
        
        if len(list_columns) != self.table.num_columns:
            print("FCan't write!!")
            return False

        new_rid = self.table.createRid()
        new_record = Record(key = cols[0], rid = new_rid, user_data = list_columns, schema_encoding = 0)
        wrote = self.table.write_record(rid=new_rid, record=new_record)
        print("Did I write?", wrote)

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
        if (len(query_columns) != self.table.num_columns) or (index_column > self.table.num_columns) or (index_column < 0):
            #print("Out of bounds or not the same number of columns")
            return False
        # error checking
        for value in query_columns:
            if value !=0 and value != 1:
                #print("Incorrect value")
                return False
        #print(f"Selecting index: {index_value}, column: {index_column}, query_columns: {query_columns}")
        rid = self.table.key_get_RID(index_value)
        #print("Rid:", rid)

        if rid is None:
            #print("Rid is none")
            return False
        
        record = self.table.read_record(rid)
        #print("record:", record)
        if record is None:
            #print("Record is none!")
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
        #print("Start of query update")
        list_columns = list(columns)
        if (len(list_columns) != self.table.num_columns) or (list_columns[0] is not None):
            #print("Out of bounds")
            return False

        rid = self.table.key_get_RID(primary_key) # base rid
        if rid is None:
            #print("Rid is none")
            return False
        
        record = self.table.read_record(rid)
        print(f"Read record {record.columns}")
        schema_encoding = record.columns[3]
        user_data = record.user_data
        #record.user_data
        #print("Preloop")
        for i in range(len(columns)):
            if columns[i] == None:
                if not get_encoding(value = schema_encoding, index = i):
                    user_data[i] = 0
                else:
                    continue
            else:
                schema_encoding = set_encoding(schema_encoding, i)
                user_data[i] = columns[i]
                #print("current data",user_data[i])
        #  create new record with updated data, still need to get the RID sorted out
        new_record = Record(key = primary_key, rid = rid, schema_encoding = schema_encoding, user_data = user_data)
        #print(f"{new_record.user_data}")
        return self.table.update_record(rid=rid, record=new_record)

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
        if start_range < 0 or end_range < 0 or aggregate_column_index < 0 or aggregate_column_index > self.table.num_columns:
            return False

        column_sum = 0
        founded_key = False
        for pagerange in self.table.page_ranges:
            for basepages in pagerange.pages:
                for i in range(512):
                    key = basepages.pages[4].read(i)
                    if key >= start_range and key <= end_range:
                        rid = basepages.pages[1].read(i)
                        record = self.table.read_record(rid)
                        user_columns = record.user_data
                        column_sum += user_columns[aggregate_column_index]
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
