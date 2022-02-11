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
<<<<<<< Updated upstream
        pass

=======
        self.tailRID = 64001
>>>>>>> Stashed changes
    """
    # internal Method
    # Read a record with specified RID
    # Returns True upon succesful deletion
    # Return False if record doesn't exist or is locked due to 2PL
    """

    def delete(self, primary_key):
        pass
    """
    # Insert a record with specified columns
    # Return True upon succesful insertion
    # Returns False if insert fails for whatever reason
    """

<<<<<<< Updated upstream
    def insert(self, *columns):
        schema_encoding = '0' * self.table.num_columns
        pass
=======
    def insert(self, *cols):
        list_columns = list(cols)
        
        if len(list_columns) != self.table.num_columns:
            return False

        schema_encoding = '0' * self.table.num_columns
        self.rid = self.table.create_rid()
        new_record = Record(key = cols[0], rid = self.rid, user_data = list_columns, schema_encoding = schema_encoding)
        self.table.write_record(self.rid, new_record)
        print()
        # print("finished")

        # not writing to database os records are not updated, i.e. need to put record into the page 
        # indirection = self.rid
        # Time = 0
        # all_columns = [indirection, globRID, Time, schema_encoding]
        # for i in range(list_columns):
        #     all_columns.append(i)
        # if has_capacity:
        #     for i in range(len(all_columns)):
        #         value = list_columns[i]
        #         self.write(value)
>>>>>>> Stashed changes

    """
    # Read a record with specified key
    # :param index_value: the value of index you want to search
    # :param index_column: the column number of index you want to search based on
    # :param query_columns: what columns to return. array of 1 or 0 values.
    # Returns a list of Record objects upon success
    # Returns False if record locked by TPL
    # Assume that select will never be called on a key that doesn't exist
    """
<<<<<<< Updated upstream

    def select(self, index_value, index_column, query_columns):
        pass
=======
    # broken
    # def select(self, index_value, index_column, query_columns):
    #     if len(query_columns) != self.table.num_columns:
    #         return False
    #     if index_column > self.table.num_columns or index_column < 0:
    #         return False
    #     for value in query_columns:
    #         if value !=0 or value != 1:
    #             return False
    #     rid = table.key_get_RID(index_value)
    #     if rid is None:
    #         return False
    #     record_list = []
    #     record = read_record(rid)
    #     for index, value in enumerate(query_columns):
    #         if value = 1:
    #             record_list.append[record.column[index+4]]
    #         else:
    #             record_list.append[None]
    #     return record_list


        # search for the base record with rid, we need to get the rid from the key
        # get the record with updated version of the key
        # return record
>>>>>>> Stashed changes
    """
    # Update a record with specified key and columns
    # Returns True if update is succesful
    # Returns False if no records exist with given key or if the target record cannot be accessed due to 2PL locking
    """

    def update(self, primary_key, *columns):
<<<<<<< Updated upstream
        pass
=======
        # check if curr_page.type == "tail"
        # if not, column.add_page(index, _type="tail")
        list_columns = list(columns)
        if len(list_columns) != table.num_columns:
            return False
        self.rid = tailRID
        rid = key_get_RID(columns(0))
        record = read_record(rid)
        record.columns[0] = tailRID
        schema_encoding = ''
        # Broken
        # for i in range(len(list_columns)-1, -1, -):
        #     if list_columns != None:
        #         schema_encoding += '1'
        #     else:
        #         schema_encoding += '0'
        new_record = Record(key = columns[0], rid = tailRID, schema_encoding = schema_encoding, columns = columns)
        self.tailrecords.append(new_record)
        indirection = rid
        Time = 0
        all_columns = [indirection, tailRID, Time, schema_encoding]
        for i in range(list_columns):
            all_columns.append(i)
        if has_capacity:
            for i in range(len(all_columns)):
                value = list_columns[i]
                self.write(value)

            return True

        return False
        # update to tail pages
        tailRID += 1
>>>>>>> Stashed changes

    """
    :param start_range: int         # Start of the key range to aggregate 
    :param end_range: int           # End of the key range to aggregate 
    :param aggregate_columns: int  # Index of desired column to aggregate
    # this function is only called on the primary key.
    # Returns the summation of the given range upon success
    # Returns False if no record exists in the given range
    """

    def sum(self, start_range, end_range, aggregate_column_index):
<<<<<<< Updated upstream
        pass
=======
        # read from base pages
        if start_range < 0 or end_range < 0:
            return False
        if aggregate_column_index < 0 or aggregate_column_index > 0:
            return False

        column_sum = 0
        founded_key = []
        for record in tailrecords:
            if start_range <= record[4] <= end_range:
                column_sum += record[aggregate_column_index+4]
                founded_key.append(record[4])

        for record in tailrecords:
            if start_range <= record[4] <= end_range:
                if record[4] not in founded_key:
                    column_sum += record[aggregate_column_index+4]

        return column_sum
>>>>>>> Stashed changes

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
