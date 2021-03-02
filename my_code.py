""" This is just some random sample code
"""
from utils import MyDataLoader

DataImporter = MyDataLoader()


def do_some_stuff_with_tables(table1, table2):
    print(f"Table 1 has {len(table1)} entries.")
    print(f"Table 2 has {len(table2)} entries.")


if __name__ == "__main__":

    table1, table2 = DataImporter.load_all()
    do_some_stuff_with_tables(table1, table2)


