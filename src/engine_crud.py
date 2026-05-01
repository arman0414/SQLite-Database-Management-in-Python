import os
import sys
import json
import re
import shutil
import csv

database = {}

current_database = None


def _cell_from_csv(s):
    s = s.strip()
    if s == '':
        return ''
    if len(s) >= 2 and s[0] == s[-1] and s[0] in "'\"":
        return s[1:-1]
    try:
        if '.' in s:
            return float(s)
        return int(s)
    except ValueError:
        return s


def _load_table_from_csv(path):
    with open(path, newline='') as f:
        rows = list(csv.reader(f))
    if not rows:
        return None
    cols = [c.strip() for c in rows[0]]
    data = [[_cell_from_csv(c) for c in row] for row in rows[1:]]
    return {'columns': cols, 'rows': data}


def database_exists(name):
    if name in database:
        return True

    if os.path.exists(name):
        database[name] = {}
        base = name
        for fn in os.listdir(base):
            path = os.path.join(base, fn)
            if not os.path.isfile(path):
                continue
            if fn.endswith('.csv'):
                tname = fn[:-4].lower()
                loaded = _load_table_from_csv(path)
                if loaded:
                    database[name][tname] = loaded
            elif fn.endswith('.json'):
                tname = fn[:-5]
                with open(path, 'r') as f:
                    database[name][tname] = json.load(f)

        return True

    return False


def table_exists(table_name):
    return table_name in database[current_database] or os.path.exists(f'{current_database}/{table_name}')


def create_database(name):
    if database_exists(name):
        print(f"!Failed to create database {name} because it already exists.")
    else:
        os.makedirs(name)
        database[name] = {}
        print(f"Database{name} created.")


def drop_database(name):
    if database_exists(name):
        shutil.rmtree(name)
        database.pop(name)
        print(f"Database {name} deleted.")
    else:
        print(f"!Failed to delete {name} because it does not exist.")


def use_database(name):
    global current_database

    if database_exists(name):
        print(f"Using database {name}.")
        current_database = name
    else:
        print(f"!Failed to use database {name} because it does not exist.")


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


def drop_table(name):
    if table_exists(name):
        database[current_database].pop(name)
        p = f'{current_database}/{name}.csv'
        if os.path.exists(p):
            os.remove(p)
        print(f"table {name} deleted.")
    else:
        print(f"!Failed to delete {name} because it does not exist.")


def col_index(table, col_name):
    for i, c in enumerate(database[current_database][table]["columns"]):
        if c.startswith(col_name):
            return i
    return None


def cmp_match(cell, op, val):
    if op == '=':
        return cell == val
    if op == '!=':
        return cell != val
    try:
        a, b = float(cell), float(val)
        if op == '>':
            return a > b
        if op == '<':
            return a < b
    except (TypeError, ValueError):
        pass
    return False


def select(name, project_cols=None, where_clause=None):
    if not table_exists(name):
        print(f"!Failed to query table {name} because it does not exist.")
        return
    colnames = database[current_database][name]["columns"]
    rows = list(database[current_database][name]["rows"])
    if where_clause:
        wc, wo, wv = parse_where(where_clause)
        wi = col_index(name, wc)
        if wi is None:
            print(f"!Failed to query table {name}.")
            return
        rows = [r for r in rows if cmp_match(r[wi], wo, wv)]
    if project_cols:
        idxs = []
        for pc in project_cols:
            j = col_index(name, pc)
            if j is not None:
                idxs.append(j)
        print(" | ".join(colnames[i] for i in idxs))
        for row in rows:
            print(" | ".join(str(row[i]) for i in idxs))
    else:
        print(" | ".join(colnames))
        for row in rows:
            print(" | ".join(str(x) for x in row))


def alter_table(name, columns):
    if table_exists(name):
        database[current_database][name]["columns"].extend(columns)

        with open(f'{current_database}/{name}', 'w') as f:
            json.dump(database[current_database][name], f)

        print(f"Table {name} modified.")
    else:
        print(f"!Failed to modify table {name} because it does not exist.")


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


def _write_csv(name):
    path = f'{current_database}/{name}.csv'
    cols = database[current_database][name]["columns"]
    rows = database[current_database][name]["rows"]
    with open(path, 'w', newline='') as f:
        w = csv.writer(f)
        w.writerow(cols)
        w.writerows(rows)


def update(name, set_column, set_value, where_column, where_op, where_value):
    if not table_exists(name):
        print(f"!Failed to update table{name} because it does not exist.")
        return
    si = col_index(name, set_column)
    wi = col_index(name, where_column)
    if si is None or wi is None:
        print(f"!Failed to update table {name}.")
        return
    n = 0
    for row in database[current_database][name]["rows"]:
        if cmp_match(row[wi], where_op, where_value):
            row[si] = set_value
            n += 1
    _write_csv(name)
    print(f"{n} rows update in {name}.")


def delete(name, column, op, value):
    if table_exists(name):
        row_index = col_index(name, column)
        if row_index is None:
            print(f"!Failed to delete from table {name}.")
            return

        rows_before = database[current_database][name]["rows"]
        n_before = len(rows_before)
        database[current_database][name]["rows"] = [
            row for row in rows_before if not cmp_match(row[row_index], op, value)
        ]
        n_after = len(database[current_database][name]["rows"])

        _write_csv(name)

        print(f"{n_before - n_after} rows deleted from {name}.")
    else:
        print(f"!Failed to delete from table {name} because it does not exist.")


def extract_value(value_str):
    value_str = value_str.strip()
    if value_str.startswith('\''):
        return value_str.removeprefix('\'').removesuffix('\'')
    elif value_str.isdigit():
        return int(value_str)
    else:
        return float(value_str)


def parse_where(s):
    s = s.strip()
    for p in ('WHERE', 'where'):
        if s.startswith(p):
            s = s[len(p):].strip()
            break
    parts = s.split()
    if len(parts) < 3:
        raise ValueError(s)
    return parts[0], parts[1], extract_value(parts[2])


line = ''
should_exit = False
if sys.stdin.isatty():
    print("Interactive CRUD — type SQL ending with ;  (or .exit). Ctrl+D to quit.", flush=True)
while not should_exit:
    try:
        chunk = input("sql> ") if sys.stdin.isatty() else input()
    except EOFError:
        break
    if chunk is None:
        continue
    chunk = chunk.strip()
    if not chunk:
        continue
    if chunk.startswith('--'):
        continue
    if chunk == '.EXIT' or chunk == '.exit':
        print('All done.')
        break
    if line:
        line += " " + chunk
    else:
        line = chunk
    if not line.endswith(';'):
        continue
    line = " ".join(line.split())
    if line.startswith('--') or len(line) == 0:
        line = ''
        continue
    if line == '.EXIT' or line == '.exit':
        print('All done.')
        break

    if not line.endswith(';'):
        continue

    if line.upper().startswith('CREATE DATABASE '):
        name = line.split(None, 2)[2].removesuffix(';').strip()
        create_database(name)
    elif line.upper().startswith('DROP DATABASE '):
        name = line.split(None, 2)[2].removesuffix(';').strip()
        drop_database(name)
    elif line.upper().startswith('USE '):
        name = line.split(None, 1)[1].removesuffix(';').strip()
        use_database(name)
    elif line.upper().startswith('CREATE TABLE '):
        rest = re.split(r'(?i)create table\s+', line, 1)[1]
        rest = rest.removesuffix(';').strip()
        openp = rest.find('(')
        nm = rest[:openp].strip().lower()
        inside = rest[openp + 1 : rest.rfind(')')]
        cols = [c.strip() for c in inside.split(',')]
        create_table(nm, cols)
    elif line.upper().startswith('DROP TABLE '):
        name = line.split(None, 2)[2].removesuffix(';').strip().lower()
        drop_table(name)
    elif line.upper().startswith('SELECT '):
        inner = re.split(r'(?i)select\s+', line, 1)[1].removesuffix(';').strip()
        proj = None
        if inner.lstrip().startswith('*'):
            rest = re.split(r'(?i)\*\s*from\s+', inner, 1)[1]
        else:
            parts = inner.split()
            proj = []
            i = 0
            while i < len(parts) and not parts[i].lower() == 'from':
                proj.append(parts[i].rstrip(','))
                i += 1
            rest = ' '.join(parts[i + 1 :])
        rest = rest.strip()
        rparts = rest.split(None, 1)
        tname = rparts[0].lower()
        where_clause = None
        if len(rparts) > 1 and rparts[1].lower().startswith('where'):
            where_clause = rparts[1]
        select(tname, proj, where_clause)
    elif line.upper().startswith('ALTER TABLE '):
        name, line = line.removeprefix('ALTER TABLE ').removeprefix('alter table ').split(' ', 1)
        line = line.removesuffix(';').removeprefix('ADD ').removeprefix('add ')

        alter_table('name', [line])
    elif line.upper().startswith('INSERT INTO') or line.upper().startswith('INSERT INTO '):
        name, line = re.split(r'(?i)insert into\s+', line, 1)[1].split(None, 1)
        name = name.lower()
        line = line.strip()
        vpart = line[line.find('(') + 1 : line.rfind(')')]
        values = [extract_value(x.strip()) for x in vpart.split(',')]
        insert_into(name, values)
    elif line.upper().startswith('UPDATE '):
        m = re.match(
            r'(?i)update\s+(\w+)\s+set\s+(\w+)\s*=\s*(.+?)\s+where\s+(.+)',
            line.removesuffix(';').strip(),
        )
        if not m:
            raise RuntimeError(f'!Failed to parse: "{line}"')
        tname, scol, sval, wpart = m.group(1), m.group(2), m.group(3).strip(), m.group(4).strip()
        sval = extract_value(sval)
        wcol, wop, wval = parse_where('where ' + wpart)
        update(tname.lower(), scol, sval, wcol, wop, wval)
    elif line.upper().startswith('DELETE FROM '):
        rest = re.split(r'(?i)delete from\s+', line, 1)[1].removesuffix(';').strip()
        rparts = rest.split(None, 1)
        tname = rparts[0].lower()
        where_column, where_op, where_value = parse_where(rparts[1])
        delete(tname, where_column, where_op, where_value)
    else:
        raise RuntimeError(f'!Failed to parse: "{line}"')

    line = ''
