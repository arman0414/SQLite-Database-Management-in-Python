import os

database = {}


def create_database(database_name):
  database[database_name] = {}


def use_database(database_name):
  if database_name not in database:
    print("Error: Database", database_name, "does not exist.")
    return False

  return True


def create_table(database_name, table_name, columns):
  if database_name not in database:
    print("Error: Database", database_name, "does not exist.")
    return False

  if table_name in database[database_name]:
    print("Error: Table", table_name, "already exists in", database_name)
    return False

  database[database_name][table_name] = {"columns": columns, "data": {}}
  print("Table", table_name, "created in", database_name)


def insert_into_table(database_name, table_name, values):
  if database_name not in database:
    print("Error: Database", database_name, "does not exist.")
    return False

  if table_name not in database[database_name]:
    print("Error: Table", table_name, "does not exist in", database_name)
    return False

  table = database[database_name][table_name]
  if len(values) != len(table["columns"]):
    print("Error: Incorrect number of values for table", table_name)
    return False

  row = {}
  for i in range(len(table["columns"])):
    column = table["columns"][i]
    value = values[i]
    row[column] = value

  table["data"][tuple(values)] = row
  print("New record inserted into", table_name)


def update_table(database_name, table_name, set_clause, where_clause):
  if database_name not in database:
    print("Error: Database", database_name, "does not exist.")
    return False

  if table_name not in database[database_name]:
    print("Error: Table", table_name, "does not exist in", database_name)
    return False

  table = database[database_name][table_name]
  updated_rows = 0

  for row_values, row_data in table["data"].items():
    if "seat" in row_data and where_clause(row_data):
      set_clause(row_data)
      updated_rows += 1

  print(updated_rows, "record(s) modified.")


def begin_transaction(database_name):
  if database_name not in database:
    print("Error: Database", database_name, "does not exist.")
    return False

  # Check if the lock file exists
  if os.path.exists(database_name + "_lock"):
    print("Error: Database", database_name, "is locked!")
    return False

  # Create the lock file
  open(database_name + "_lock", "w").close()
  print("Transaction starts.")
  return True


def commit_transaction(database_name):
  if database_name not in database:
    print("Error: Database", database_name, "does not exist.")
    return False

  # Remove the lock file
  os.remove(database_name + "_lock")
  print("Transaction committed.")


def rollback_transaction(database_name):
  if database_name not in database:
    print("Error: Database", database_name, "does not exist.")
    return False

  # Remove the lock file
  os.remove(database_name + "_lock")
  print("Transaction aborted.")


def select_data(database_name, table_name):
  if database_name not in database:
    print("Error: Database", database_name, "does nort exist")
    if database_name not in database:
      print("Error: Database", database_name, "does not exist.")
    return False

  if table_name not in database[database_name]:
    print("Error: Table", table_name, "does not exist in", database_name)
    return False

  table = database[database_name][table_name]

  # Print table header
  columns = table["columns"]
  print("|".join(columns))

  # Print table data
  for row_values, row_data in table["data"].items():
    values = [str(row_data[column]) for column in columns]
    print("|".join(values))


# Test script
create_database("CS457_PA4")
use_database("CS457_PA4")
create_table("CS457_PA4", "Flights", ["seat int", "status int"])
insert_into_table("CS457_PA4", "Flights", [22, 0])
insert_into_table("CS457_PA4", "Flights", [23, 1])
begin_transaction("CS457_PA4")
update_table("CS457_PA4", "Flights", lambda row: row.update({"status": 1}),
             lambda row: row["seat"] == 22)
commit_transaction("CS457_PA4")
use_database("CS457_PA4")
begin_transaction("CS457_PA4")
update_table("CS457_PA4", "Flights", lambda row: row.update({"status": 1}),
             lambda row: row["seat"] == 22)
rollback_transaction("CS457_PA4")
select_data("CS457_PA4", "Flights")
commit_transaction("CS457_PA4")
select_data("CS457_PA4", "Flights")
