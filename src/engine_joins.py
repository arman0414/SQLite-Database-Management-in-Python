import os
import json
import shutil
import re

database = {}
current_database = None


def database_exists(name):
    if name in database:
        return True
    if os.path.exists(name):
        database[name] = {}
        for table in os.listdir(name):
            path = os.path.join(name, table)
            if os.path.isfile(path) and not table.endswith('.csv'):
                with open(path, 'r') as f:
                    database[name][table] = json.load(f)
        return True
    return False


def table_exists(table_name):
    return table_name in database[current_database] or os.path.exists(
        os.path.join(current_database, table_name)
    )


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
        database.pop(name, None)
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


def col_index(table, col_name):
    col_name = col_name.lower()
    for i, c in enumerate(database[current_database][table]['columns']):
        part = c.split()[0].lower()
        if part == col_name or c.lower().startswith(col_name):
            return i
    return None


def create_table(name, columns):
    name = name.lower()
    cols = [c.strip() for c in columns]
    if table_exists(name):
        print(f"!Failed to create table {name} because it already exists.")
    else:
        database[current_database][name] = {'columns': cols, 'rows': []}
        print(f"Table {name} created.")


def drop_table(name):
    name = name.lower()
    if table_exists(name):
        database[current_database].pop(name, None)
        p = os.path.join(current_database, name)
        if os.path.exists(p):
            os.remove(p)
        print(f"table {name} deleted.")
    else:
        print(f"!Failed to delete {name} because it does not exist.")


def extract_value(value_str):
    value_str = value_str.strip()
    if value_str.startswith("'"):
        return value_str.removeprefix("'").removesuffix("'")
    if value_str.isdigit():
        return int(value_str)
    return float(value_str)


def insert_into(name, values):
    name = name.lower()
    if table_exists(name):
        if len(values) != len(database[current_database][name]['columns']):
            print(
                f"!Failed to insert into table {name} because the number of values does not match the number of columns."
            )
            return
        database[current_database][name]['rows'].append(values)
        print(f"Inserted values into table {name}.")
    else:
        print(f"!Failed to insert into table {name} because it does not exist.")


def parse_on_condition(cond):
    left, right = [x.strip() for x in cond.split('=', 1)]
    la, lc = left.split('.', 1)
    ra, rc = right.split('.', 1)
    return (la.strip(), lc.strip()), (ra.strip(), rc.strip())


def run_join_query(
    left_table,
    left_alias,
    right_table,
    right_alias,
    on_cond,
    join_kind,
):
    left_table = left_table.lower()
    right_table = right_table.lower()
    (la, lc), (ra, rc) = parse_on_condition(on_cond)
    assert la == left_alias and ra == right_alias

    lcols = database[current_database][left_table]['columns']
    rcols = database[current_database][right_table]['columns']
    li = col_index(left_table, lc)
    ri = col_index(right_table, rc)
    if li is None or ri is None:
        print(f"Failed to query: bad join columns.")
        return

    lrows = database[current_database][left_table]['rows']
    rrows = database[current_database][right_table]['rows']

    header = lcols + rcols
    print(' | '.join(header))

    if join_kind == 'inner':
        for lr in lrows:
            for rr in rrows:
                if lr[li] == rr[ri]:
                    print(' | '.join(str(x) for x in lr + rr))
    else:
        nr = len(rcols)
        for lr in lrows:
            matches = [rr for rr in rrows if lr[li] == rr[ri]]
            if not matches:
                pad = [''] * nr
                print(' | '.join(str(x) for x in list(lr) + pad))
            else:
                for rr in matches:
                    print(' | '.join(str(x) for x in lr + rr))


def run_select(line):
    line = line.removesuffix(';').strip()
    m = re.match(r'(?i)select\s+(.+?)\s+from\s+(.+)$', line)
    if not m:
        print(f'!Failed to parse select: "{line}"')
        return
    proj = m.group(1).strip()
    from_clause = m.group(2).strip()
    if proj != '*':
        print('!Only select * is supported in joins engine.')
        return

    m_lo = re.match(
        r'(?i)^(\w+)\s+(\w+)\s+left\s+outer\s+join\s+(\w+)\s+(\w+)\s+on\s+(.+)$',
        from_clause,
    )
    if m_lo:
        t1, a1, t2, a2, on = m_lo.groups()
        run_join_query(t1, a1, t2, a2, on, 'left')
        return

    m_ij = re.match(
        r'(?i)^(\w+)\s+(\w+)\s+inner\s+join\s+(\w+)\s+(\w+)\s+on\s+(.+)$',
        from_clause,
    )
    if m_ij:
        t1, a1, t2, a2, on = m_ij.groups()
        run_join_query(t1, a1, t2, a2, on, 'inner')
        return

    m_cj = re.match(
        r'(?i)^(\w+)\s+(\w+)\s*,\s*(\w+)\s+(\w+)\s+where\s+(.+)$',
        from_clause,
    )
    if m_cj:
        t1, a1, t2, a2, cond = m_cj.groups()
        run_join_query(t1, a1, t2, a2, cond, 'inner')
        return

    print(f'!Failed to parse from clause: "{from_clause}"')


line = ''
while True:
    try:
        chunk = input()
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
        line += ' ' + chunk
    else:
        line = chunk
    if not line.endswith(';'):
        continue
    line = ' '.join(line.split())
    if line.startswith('--') or len(line) == 0:
        line = ''
        continue
    if line == '.EXIT' or line == '.exit':
        print('All done.')
        break

    if line.upper().startswith('CREATE DATABASE '):
        name = line.split(None, 2)[2].removesuffix(';').strip()
        create_database(name)
    elif line.upper().startswith('DROP DATABASE '):
        name = line.split(None, 2)[2].removesuffix(';').strip()
        drop_database(name)
    elif line.upper().startswith('USE '):
        name = line.split(None, 1)[1].removesuffix(';').strip()
        use_database(name)
    elif re.match(r'(?i)^CREATE TABLE\s+', line):
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
    elif re.match(r'(?i)^INSERT\s+INTO\s+', line):
        name, rest = re.split(r'(?i)insert into\s+', line, 1)[1].split(None, 1)
        name = name.lower()
        rest = rest.strip()
        vpart = rest[rest.find('(') + 1 : rest.rfind(')')]
        values = [extract_value(x.strip()) for x in vpart.split(',')]
        insert_into(name, values)
    elif re.match(r'(?i)^SELECT\s+', line):
        run_select(line)
    else:
        print(f'!Failed to parse: "{line}"')

    line = ''
