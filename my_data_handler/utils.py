""" These are my utils
"""
import json
import pandas as pd
import shutil


class MyDataHandler:

    def __init__(self):
        self.config = self.get_config_from_json()
        self.list_of_tables = [table for table in self.config.keys()]
        self.connect_to_remote()

    def get_config_from_json(self):
        with open("../my_data_versions.json", "r") as config:
            return json.load(config)

    def get_version(self, table):
        return self.config.get(table, -1)

    def connect_to_remote(self):
        """ tbd """
        pass


class MyDataLoader(MyDataHandler):
    """
    My DataLoader loads my sample files such that i can develop cool stuff on my machine.
    If the versions i specified in the json-config is not available here, it will be downloaded.
    """

    def __init__(self):
        super().__init__()

    def load(self, table):
        version = self.get_version(table)
        if version == -1:
            raise KeyError("Table not defined in config file!")
        else:
            try:
                table = pd.read_csv(f"my_local_cache/{table}_v{version}.csv")
                return table
            except:
                print(f"Table is not cached. So let's download it.")
                self.download_table(table, version)
                table = pd.read_csv(f"my_local_cache/{table}_v{version}.csv")
                return table

    def load_all(self):
        tables = []
        for table in self.list_of_tables:
            tables.append(self.load(table))
        return tuple(tables)

    def download_table(self, table, version):
        try:
            shutil.copyfile(src=f"remote_store/{table}_v{version}.csv",
                            dst=f"my_local_cache/{table}_v{version}.csv")
        except:
            raise KeyError("Specific version not found in remote store!")


class MyUploader(MyDataHandler):
    """ My Uploader is able to connect to remote and upload local edited files."""

    def __init__(self):
        super().__init__()
        self.list_of_remote_files = self._list_of_remote_files()

    def _list_of_remote_files(self):
        """tbd"""
        return []

    def upload_table(self, table, version):

        if f"{table}_v{version}" in self.list_of_remote_files:
            print("Table version already exists at remote!")
        else:
            try:
                shutil.copyfile(dst=f"remote_store/{table}_v{version}.csv",
                                src=f"my_local_cache/{table}_v{version}.csv")
            except:
                raise KeyError("Specific version not found in local store!")

    def upload_all(self):
        tables = []
        for table in self.list_of_tables:
            version = self.get_version(table)
            self.upload_table(table, version)
        return tuple(tables)
