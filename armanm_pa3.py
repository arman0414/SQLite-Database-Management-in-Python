import os
import json
import shutil
import re

#Dictionary
database={}


current_database= None

def database_exists(name):
    if name in database:
        return True
    
    if os.path.exists(name):
        database[name] = {}
        
        for table in os.listdir(f'{name}'):
            with open(f'{name}/{table}', 'r') as f:
                database[name][table] = json.load(f)
                
        return True
        
    return False
    
def table_exists(table_name):
    return table_name in database[current_database] or os.path.exists(f'{current_database}/{table_name}')

#create database 
def create_database(name):
    if database_exists(name):
        print(f"!Failed to create database {name} because it already exists.")
    else:
        os.makedirs(name)
        database[name] = {}
        print(f"Database{name} created.")

#drop database
def drop_database(name):
    if database_exists(name):
        shutil.rmtree(name)
        database.pop(name)
        print(f"Database {name} deleted.")
    else:
        print(f"!Failed to delete {name} because it does not exist.")

#use database
def use_database(name):
    global current_database
    
    if database_exists(name):
        print(f"Using database {name}.")
        current_database = name
    else:
        print(f"!Failed to use database {name} because it does not exist.")

#create table
def create_table(line):
    # Extract table name and columns from input line using regular expressions
    match = re.match(r'create table (\w+)\((.*?)\);', line)
    if not match:
        print(f'!Failed to parse: "{line}"')
        return
    name = match.group(1).lower()
    columns = match.group(2).strip()

    if table_exists(name):
        print(f"!Failed to create table {name} because it already exists.")
    else:
        database[current_database][name] = {}
        # Initialize the dictionary for the table with attribute names as keys and empty lists as values
        database[current_database][name]["columns"] = columns.split(",")  # split columns string into a list of attribute names
        database[current_database][name]["rows"] = []
        print(f"Table {name} created.")


#drop table
def drop_table(name):
    name = name.lower()
    if table_exists(name):
        database[current_database].pop(name)
        
        os.remove(f'{current_database}/{name}')
        
        return f"table {name} deleted."
    else:
        print(f"!Failed to delete {name} because it does not exist.")

#select tables
def join_tables(table_name, join_table_name, join_column_name):
    table_name = table_name.lower()
    join_table_name = join_table_name.lower()
    join_column_name = join_column_name.lower()

    if not table_exists(table_name):
        raise ValueError(f"Failed to query table {table_name} because it does not exist.")
    if not table_exists(join_table_name):
        raise ValueError(f"Failed to query table {join_table_name} because it does not exist.")
    if join_column_name not in database[current_database][table_name]["columns"] or join_column_name not in database[current_database][join_table_name]["columns"]:
        raise ValueError(f"Failed to query table {table_name} and {join_table_name} because column {join_column_name} does not exist in one of the tables.")

    # Find the index of the join column in both tables
    result = []
    name_join_col_indx = database[current_database][table_name]["columns"].index(join_column_name)
    join_name_join_col1_indx = database[current_database][join_table_name]["columns"].index(join_column_name)

    # Perform join operation
    for row1 in database[current_database][table_name]["rows"]:
        for row2 in database[current_database][join_table_name]["rows"]:
            if row1[name_join_col_indx] == row2[join_name_join_col1_indx]:
                result.append(row1 + row2)

    return result


def select(name):
    name = name.lower()
    if not table_exists(name):
        print(f"Failed to query table {name} because it does not exist.")
    else:
        columns = database[current_database][name]["columns"]
        print(" | ".join(columns))

        join_table_name = input("Enter the name of the table to be joined: ").lower()
        join_column_name = input("Enter the column name for joining: ").lower()

        # Check if the join table exists
        if not table_exists(join_table_name):
            print(f"Failed to query table {join_table_name} because it does not exist.")
            return
        # Check if the join column exists in both tables
        if join_column_name not in database[current_database][name]["columns"] or join_column_name not in database[current_database][join_table_name]["columns"]:
            print(f"Failed to query table {name} and {join_table_name} because column {join_column_name} does not exist in one of the tables.")
            return

        # Call the join_tables function to perform the join operation
        result = join_tables(name, join_table_name, join_column_name)
        # Print the result
        for row in result:
            print(" | ".join(row))


#alter table
def alter_table(name, columns):
    name = name.lower()
    if table_exists(name):
        database[current_database][name]["columns"].extend(columns)
        
        with open(f'{current_database}/{name}', 'w') as f:
            json.dump(database[current_database][name], f)
            
        print(f"Table {name} modified.")
    else:
        print(f"!Failed to modify table {name} because it does not exist.")
#insert into
def insert_into(name, values):
    if table_exists(name):
        if len(values) != len(database[current_database][name]["columns"]):
            print(f"!Failed to insert into table {name} because the number of values does not match the number of columns.")
            return
        # Append the row data as a list to the "rows" key in the table's dictionary
        database[current_database][name]["rows"].append(values)
        print(f"Inserted values into table {name}.")
    else:
        print(f"!Failed to insert into table {name} because it does not exist.")

#update table
def update(name,set_column, set_value, where_column, where_value):
    name = name.lower()
    if table_exists(name):
        row_index = None
        for i, column in enumerate(database[current_database][name]["columns"]):
            if column.startswith(set_column):
                row_index = i
        
        for row in database[current_database][name]["rows"]:
            if row[database[current_database][name]["columns"].index(where_column)] == where_value:
                row[row_index] = set_value
                
        with open(f'{current_database}/{name}', 'w') as f:
            json.dump(database[current_database][name], f)
            
        print(f"Table {name} updated.")
        
    else:
        print(f"!Failed to update table {name} because it does not exist.")


#delete from table
def delete(name, column, value):
    name = name.lower()
    if table_exists(name):
        row_index = None
        for i, c in enumerate(database[current_database][name]["columns"]):
            if c.startswith(column):
                row_index = i
                
        rows = database[current_database][name]["rows"]
        rows_before = len(rows)
        database[current_database][name]["rows"] = [row for row in rows if row[row_index] != value]
        rows_after = len(database[current_database][name]["rows"])
        with open(f'{current_database}/{name}', 'w') as f:
            json.dump(database[current_database][name], f)

        print(f"{rows_before - rows_after} rows deleted from {name}.")

    else:
        print(f"!Failed to delete from table {name} because it does not exist.")

# Read a sql value
def extract_value(value_str):
    value_str = value_str.strip()
    if value_str.startswith('\''):
        return value_str.removeprefix('\'').removesuffix('\'')
    elif value_str.isdigit():
        return int(value_str)
    else:
        return float(value_str)
                
# Parse `WHERE <column_name> <op> <column_value>`

def parse_where(str):
    str = str.removeprefix('WHERE').removeprefix('where')
     
    column_name, op, column_value = str.split()
    column_value = extract_value(column_value)
    return column_name, op, column_value
## Perform the join operation and update rows in the main table
def update_with_join(table_name, join_table_name, on_column_name1, on_column_op, on_column_value):
    # Perform the join operation and update rows in the main table
    if not table_exists(table_name):
        print(f"!Failed to update table {table_name} because it does not exist.")
    elif not table_exists(join_table_name):
        print(f"!Failed to update table {table_name} because join table {join_table_name} does not exist.")
    else:
         
        print(f"Updated rows in table {table_name} with join table {join_table_name}.")



line = ''
should_exit = False
while not should_exit:
    line += input()
    if line.startswith('--') or len(line) == 0:
        line = ''
        continue
    if line == '.EXIT' or line == '.exit':
        print('All done.')
        break
        
    if not line.endswith(';'):
        continue
        
    if line.startswith('CREATE DATABASE '):
        name = line.removeprefix('CREATE DATABASE ').removesuffix(';')
        create_database(name)
    elif line.startswith('DROP DATABASE '):
        name = line.removeprefix('DROP DATABASE ').removesuffix(';')
        drop_database(name)
    elif line.startswith('USE '):
        name = line.removeprefix('USE ').removesuffix(';')
        use_database(name)
    elif line.startswith('CREATE TABLE '):
        (name, line) = line.removeprefix('CREATE TABLE ').split(' ', 1)
        line = line.removesuffix(';').removesuffix(')').removeprefix('(')
        columns = line.split(', ')
        
        create_table(name.lower(), columns)
    elif line.startswith('DROP TABLE '):
        name = line.removeprefix('DROP TABLE ').removesuffix(';')
        drop_table(name)
    elif line.startswith('SELECT ') or line.startswith('select '):
        line = line.removeprefix('SELECT ').removeprefix('select ').removesuffix(';')
        columns = []
        if line[0] == '*':
            line = line.removeprefix('* ').removeprefix('from ').removeprefix('FROM ')
        else:
            while not line.startswith('from'):
                column, line = line.split(' ', 1)
                columns.append(column)
            line = line.removeprefix('from ').removeprefix('FROM ')
            
        line = line.split(' ', 1)
        if len(line) == 1:
            name = line[0]
        else:
            name = line[0]
            where_data = parse_where(line[1])
        
        if 'JOIN' in line[1].upper():
            join_table_name = ''
            join_column_name1 = ''
            join_column_name2 = ''

            if 'INNER JOIN' in line[1].upper():
                (join_table_name, line[1]) = line[1].removeprefix('INNER JOIN ').removeprefix('inner join ').split(' ', 1)
            else:
                (join_table_name, line[1]) = line[1].removeprefix('JOIN ').removeprefix('inner join ').split(' ', 1)
            (on_column_name1, line[1]) = line[1].removeprefix('ON ').removeprefix('on ').split(' ', 1)
            (on_column_op, on_column_value) = parse_where(line[1]) 

            join_table_name = join_table_name.lower()
            on_column_name1 = on_column_name1.lower()
            update_with_join(name.lower(), join_table_name, on_column_name1, on_column_op, on_column_value) 
        else:
            select(name.lower())
    elif line.startswith('ALTER TABLE '):
        name, line = line
