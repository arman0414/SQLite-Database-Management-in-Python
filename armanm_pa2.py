import os
import json
import shutil
import csv

#Dictionary
database={}


current_database=None

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



def create_table(name, columns):
    if table_exists(name):
        print(f"!Failed to create table {name} because it already exists.")
    else:
        database[current_database][name] = {}
        database[current_database][name]["columns"] = columns
        database[current_database][name]["rows"] = []
        print(f"Table {name} created.")

        with open(f'{current_database}/{name}.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(columns)


#drop table
def drop_table(name):
    if table_exists(name):
        database[current_database].pop(name)
        
        os.remove(f'{current_database}/{name}')
        
        print(f"table {name} deleted.")
    else:
        print(f"!Failed to delete {name} because it does not exist.")

#select tables
def select(name):
    if not table_exists(name):
        print(f"!Failed to query table {name} because it does not exist.")
    else:
        columns = database[current_database][name]["columns"]
        print(" | ".join(columns))

        with open(f'{current_database}/{name}.csv', 'r', newline='') as f:
            reader = csv.reader(f)
            for row in reader:
                print(" | ".join(row))


#alter table
def alter_table(name, columns):
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

        database[current_database][name]["rows"].append(values)

        with open(f'{current_database}/{name}.csv', 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(values)

        print(f"1 row inserted into {name}.")
    else:
        print(f"!Failed to insert into table {name} because it does not exist.")

#update table
def update(name,set_column, set_value, where_column, where_value):
    if table_exists(name):
        row_index = None
        for i, column in enumerate(database[current_database][name]["columns"]):
            if column.startswith(set_column):
                row_index = i
        
        for row in database[current_database][name]["rows"]:
            if row[row_index] == where_value:
                row[set_column] = set_value

        with open(f'{current_database}/{name}', 'w') as f:
            json.dump(database[current_database][name], f)
        print(f"{len(database[current_database][name]['rows'])} rows update in {name}.")
    else:
        print(f"!Failed to update table{name} because it does not exist.")


#delete from table
def delete(name, column, value):
    if table_exists(name):
        row_index = None
        for i, c in enumerate(database[current_database][name]["columns"]):
            if c.startswith(column):
                row_index = i

        rows_before = len(database[current_database][name]["rows"])
        database[current_database][name]["rows"] = [row for row in database[current_database][name]["rows"] if row[row_index] != value]
        rows_after = len(database[current_database][name]["rows"])

        with open(f'{current_database}/{name}.csv', 'w', newline='') as f:
            writer = csv.writer(f)
            writer.writerow(database[current_database][name]["columns"])
            writer.writerows(database[current_database][name]["rows"])

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
        
        select(name.lower())
    elif line.startswith('ALTER TABLE '):
        name, line = line.removeprefix('ALTER TABLE ').split(' ', 1)
        line = line.removesuffix(';').removeprefix('ADD ')
        
        alter_table('name', [line])
    elif line.startswith('INSERT INTO') or line.startswith('insert into '):
        name, line = line.removeprefix('INSERT INTO ').removeprefix('insert into ').split(' ', 1)
        values = line.strip('values(').removesuffix(');').split(',')
        values = [extract_value(value) for value in values]
        
        insert_into(name.lower(), values)
    elif line.startswith('UPDATE ') or line.startswith('update '):
        (name, line) = line.removeprefix('UPDATE ').removeprefix('update ').split(' ', 1)
        (set_column_name, line) = line.removeprefix('set ').split(' ', 1)
        (set_column_value, line) = line.removeprefix('= ').split(' ', 1)
        set_column_value = extract_value(set_column_value)
        
        where_column_name, where_column_op, where_column_value = parse_where(line)
        
        update(name.lower(), set_column_name, set_column_value, where_column_name, where_column_value)
    elif line.startswith('DELETE FROM ') or line.startswith('delete from '):
        (name, line) = line.removeprefix('DELETE FROM ').removeprefix('delete from ').removesuffix(';').split(' ', 1)
        
        where_column_name, where_column_op, where_column_value = parse_where(line)
        
        delete(name.lower(), where_column_name, where_column_value)
    else:
        raise RuntimeError(f'!Failed to parse: "{line}"')
        
    line = ''