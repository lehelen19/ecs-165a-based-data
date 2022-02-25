from lstore.table import Table, Bufferpool
import os
import pickle

class Database():

    def __init__(self):
        self.table = {}
        self.tableDirectory = {}
        self.bufferpoolPath = None
        self.rootDirectory = None

    def open(self, path):
        self.bufferpoolPath = Bufferpool(path)
        if not os.path.isdir(path):
            os.mkdir(path)
            self.rootDirectory = path
        else:
            self.rootDirectory = path
            for files in os.scandir(path):
                tableDirectoryPath = f"{path}\\tableDirectory.pkl"
                if files.path == tableDirectoryPath:
                    with open(tableDirectoryPath, "rb") as data:
                        self.tableDirectory = pickle.load(data)
                else:
                    continue
            for tab in self.tableDirectory:
                tablePath = self.table_directory[tab].get("path")
                numCol = self.table_directory[tab].get("num_columns")
                key = self.table_directory[tab].get("key")
                table = Table(name=tab, num_columns=numCol, key=key, path=path, bufferpool=self.bufferpoolPath)

                pageDirectoryPath= f"{tableDirectoryPath}/page_directory.pkl"
                with open(pageDirectoryPath, "rb") as page_directory:
                    table.page_directory = pickle.load(page_directory)
                
                tableData = table.page_directory["data"]
                table.name = tableData["name"]
                table.key = tableData["key"]
                table.total_columns = tableData["total_columns"]
                table.num_columns = tableData["num_columns"]
                table.num_records = tableData["num_records"]
                table.page_directory = tableData["page_directory"]
                table.path = tableData["path"]
                table.bufferpool= tableData["bufferpool"]

                with open(f"{tableDirectoryPath}/index.pkl", "rb") as index:
                    table.index = pickle.load(index)
                self.table[tab] = table


    def close(self):
        with open(f"{self.rootDirectory}/tableDirectory.pkl", "wb") as tableDirectory:
            pickle.dump(self.table_directory, tableDirectory)

        for values in self.table_directory.values():
            name = values.get("name")
            table = self.tables[name]
            tableData = {"name": table.name, "key": table.key,  "num_colums": table.num_columns, "total_colums": table.total_columns, "num_records": table.num_records, "page_directory": table.page_directory, "path": table.path, "bufferpool": table.bufferpool}
            table.page_directory["data"] = tableData
        with open f"{table.path}/page_directory.pkl", "wb") as page_directory:
            pickle.dump(table.page_directory, page_directory)
        with  open(f"{table.path}/index.pkl", "wb") as index:
            pickle.dump(table.index, index)
    
        # self.bufferpool.commit_all_frames()

    """
    # Creates a new table
    :param name: string         #Table name
    :param num_columns: int     #Number of Columns: all columns are integer
    :param key: int             #Index of table key in columns
    """
    def create_table(self, name, key_index, num_columns):
        path = f"{self.rootDirectory}/{name}"
        if not os.path.isdir(table_path_name):
            os.mkdir(table_path_name)
        else: 
            raise Exception("Table already exists")

        table = Table(name, key_index, num_columns)
        self.tables[name] = table
        self.tableDirectory[name] = {"path": path, "name": name, "num_columns": num_columns, "key": key}
        return table

    """
    # Deletes the specified table
    """
    def drop_table(self, name):
        self.tables[name] = None
        return f"Table {name} has been dropped."


    """
    # Returns table with the passed name
    """
    def get_table(self, name):
        return self.tables[name]
