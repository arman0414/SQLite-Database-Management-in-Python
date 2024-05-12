 
import os
import json
import shutil

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
        database[current_database][name]={}
        database[current_database][name]["columns"]=columns
        database[current_database][name]["rows"] = []
        print(f"Table {name} created.")
        
        with open(f'{current_database}/{name}', 'w') as f:
            json.dump(database[current_database][name], f)

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
        for row in database[current_database][name]["rows"]:
            print(" | ".join(str(val) for val in row))

#alter table
def alter_table(name, columns):
    if table_exists(name):
        database[current_database][name]["columns"].extend(columns)
        
        with open(f'{current_database}/{name}', 'w') as f:
            json.dump(database[current_database][name], f)
            
        print(f"Table {name} modified.")
    else:
        print(f"!Failed to modify table {name} because it does not exist.")



should_exit = False
while not should_exit:
    line = input()
    if line.startswith('--') or len(line) == 0:
        continue
    if line == '.EXIT':
        print('All done.')
        break
        
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
        
        create_table(name, columns)
    elif line.startswith('DROP TABLE '):
        name = line.removeprefix('DROP TABLE ').removesuffix(';')
        drop_table(name)
    elif line.startswith('SELECT * FROM '):
        name = line.removeprefix('SELECT * FROM ').removesuffix(';')
        select(name)
    elif line.startswith('ALTER TABLE '):
        (name, line) = line.removeprefix('ALTER TABLE ').split(' ', 1)
        line = line.removesuffix(';').removeprefix('ADD ')
        
        alter_table('tbl_1', [line])
    else:
        raise RuntimeError(f'!Failed to parse: "{line}"')