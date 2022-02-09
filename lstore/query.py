from lstore.table import Table, Record
from lstore.index import Index


class Query:
    """
    # Creates a Query object that can perform different queries on the specified table
    Queries that fail must return False
    Queries that succeed should return the result or True
    Any query that crashes (due to exceptions) should return False
    """

    def __init__(self, table):
        self.table = table
        pass

    # globals rid, every inter += 1
    globRID = 0;
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        pass
        rid = table.key_get_RID(primary_key)
        if rid is None:
            return False
        # table.
        record = self.table.read_record(rid)
        record.column[1] = '*'
        if (record.column[1] != record.column[0]): #if there are any updates check
            tail_record = self.table.read_record(record.column[0])
            while tail_record.column[1] != tail_record.column[0]:
                tail_record.column[1] = '*'
                tail_record = self.table.read_record(rid[0])
            tail_record.column[1] = '*'
        return True





    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

    def insert(self, *columns):
        # insert into base page, for read only access later
        # list_columns = list(columns)
        if len(columns) != table.num_columns:
            return False
        schema_encoding = '0' * self.table.num_columns
        self.rid = globRID
        new_record = Record(key = columns[0], rid = globRID, schema_encoding = schema_encoding, columns = list(columns))
        if has_capacity:
            for i in range(len(columns)):
                self.columns[i+4].append(columns[i])
            return True

        return False

        globRID += 1
        pass

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
        pass
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
        # update to tail pages
        pass

    """
    :param start_range: int         # Start of the key range to aggregate
    :param end_range: int           # End of the key range to aggregate
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
        # read from base pages
        pass

    """
    incremenets one column of the record
    this implementation should work if your select and update queries already work
    :param key: the primary of key of the record to increment
    :param column: the column to increment
    # Returns True is increment is successful
    # Returns False if no record matches key or if target record is locked by 2PL.
    """

    def increment(self, key, column):
        r = self.select(key, self.table.key, [1] * self.table.num_columns)[0]
        if r is not False:
            updated_columns = [None] * self.table.num_columns
            updated_columns[column] = r[column] + 1
            u = self.update(key, *updated_columns)
            return u
        return False
