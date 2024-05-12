#Arman Malik(657)
import sqlite3

# Connect to the database
conn = sqlite3.connect('db_tpch.db')
cursor = conn.cursor()

# Createing table
cursor.execute('CREATE TABLE Part (Partkey int, Size int)')


# Inserting records or data values
cursor.execute('CREATE TABLE IF NOT EXISTS Part (Partkey int, Size int)')
cursor.execute('INSERT INTO Part VALUES (1, 7)')
cursor.execute('INSERT INTO Part VALUES (2, 1)')
cursor.execute('INSERT INTO Part VALUES (3, 21)')
cursor.execute('INSERT INTO Part VALUES (4, 14)')
cursor.execute('INSERT INTO Part VALUES (5, 15)')
cursor.execute('INSERT INTO Part VALUES (6, 4)')
cursor.execute('INSERT INTO Part VALUES (7, 45)')
cursor.execute('INSERT INTO Part VALUES (8, 41)')
cursor.execute('INSERT INTO Part VALUES (9, 12)')
cursor.execute('INSERT INTO Part VALUES (10, 44)')

# Print output
print(" Database db_tpch created")
print(" Using database db_tpch.")
print(" Table Part created.")

print(" 1 new record inserted.")
print(" 1 new record inserted.")
print(" 1 new record inserted.")
print(" 1 new record inserted.")
print(" 1 new record inserted.")
print(" 1 new record inserted.")
print(" 1 new record inserted.")
print(" 1 new record inserted.")
print(" 1 new record inserted.")
print(" 1 new record inserted.")
# Executing queries and Printing Count (*)
cursor.execute('SELECT COUNT(Size) FROM Part')
COUNT_result = cursor.fetchone()[0]
print(" COUNT(*)")
print(str(COUNT_result))

# Executing queries and Printing AVG (size)
cursor.execute('SELECT AVG(Size) FROM Part')
AVG_result = cursor.fetchone()[0]
print(" AVG(Size)")
print(str(AVG_result))

# Execute queries and Printing MAX(Size)
cursor.execute('SELECT MAX(Size) FROM Part')
max_result = cursor.fetchone()[0]
print(" MAX(Size)")
print(str(max_result))
