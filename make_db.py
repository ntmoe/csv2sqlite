import sqlite3
from csv2sqlite import *

conn = sqlite3.connect('data.db')
c = conn.cursor()

temp_table_name = 'temp_data'
tablename = 'data'

with open('vehicles.csv') as fin:
    s = fin.read()
    import_csv_str_to_db(s,',',temp_table_name,conn)

detect_column_types(conn, temp_table_name)

create_table(conn, tablename)

# Now copy all the old data to the new table
copy_table(conn, temp_table_name, tablename)

# Get rid of the temporary table
c.execute('Drop table %s' % (temp_table_name))
c.execute('Drop table %s' % ('temp_pragma',))

conn.commit()

