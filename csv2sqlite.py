import sqlite3
import csv
import re
from collections import OrderedDict
import datetime

def import_file(filename):
    fin = open(filename)
    s = fin.read()
    fin.close()
    return s

def sql_list(l):
    """Returns a string that can be put into an SQL statement.
    """
    return u', '.join('"' + unicode(i) + '"' for i in l)

realpat = re.compile(r"""^      # The start of a string
                         -?     # 0 or 1 negative signs
                         \d*    # 0 or more digits
                         \.     # the decimal point
                         \d*    # 0 or more digits
                         $      # the end of the string
                    """, re.X)

intpat = re.compile(r"""^      # The start of a string
                         -?     # 0 or 1 negative signs
                         \d*    # 0 or more digits
                         $      # the end of the string
                    """, re.X)

def is_int(s):
    return intpat.match(s)

def is_real(s):
    return realpat.match(s)

def find_list_type(col):
    col_is_real = False
    col_is_int = False
    col_is_string = False
    for i in col:
        if i != u'':
            if is_int(i):
                col_is_int = True
            elif is_real(i):
                col_is_real = True
            else:
                return u'text'
            # Now check if we have agreement with what we've found so far
            if (col_is_int == True) and (col_is_real == True):
                col_is_real = True      # If there is a mix of ints and reals, the column is real
                col_is_int = False

    if col_is_int == True:
        return u'integer'
    elif col_is_real == True:
        return u'real'
    else:
        return u'text'

def import_DictReader_to_db(reader, dbname, c):
    ld = [record for record in reader]

    for record in ld:
        keys = []
        values = []
        for k, v in record.iteritems():
            keys.append(k)
            values.append(v)

        cmd = u"INSERT INTO %s (%s) values (%s)" % (dbname, sql_list(keys), sql_list(values))

        try:
            c.execute(cmd)
        except:
            print 'This command failed:'
            print cmd
            break

def import_csv_str_to_db(s, delim, tablename, conn):
    c = conn.cursor()
    l = s.splitlines()
    r = csv.DictReader(l, delimiter=delim)

    # Create temporary table.
    # First, create a list of header definitions
    header_defs = []
    for label in r.fieldnames:
        # If we get an ID row from the csv, treat it as a primary key
        if (label == 'id' or label == 'ID'):
            header_defs.append(u'id integer primary key')
        else:
            header_defs.append(label + u' text')
    
    header = u', '.join(header_defs)
    cmd = u'CREATE TEMP TABLE %s (%s)' % (tablename, header)
    c.execute(cmd)

    import_DictReader_to_db(r, tablename, c)
    conn.commit()

    # # Create a new table with appropriate containers
    # header = ', '.join(['%s %s' % (label[0], label[1]) for label in types])
    # cmd = 'CREATE TABLE %s (id integer primary key, %s)' % (tablename, header,)
    # c.execute(cmd)
    # r = csv.DictReader(l, delimiter=';')
    # import_DictReader_to_db(r, tablename, c)
    # conn.commit()
    # c.execute('DROP TABLE %s' % (temp_table_name,))
    # conn.commit()

def detect_column_types(conn, tablename, pragmaname=u'temp_pragma'):
    conn.row_factory = sqlite3.Row
    c = conn.cursor()

    # Get names of each column
    c.execute(u"pragma table_info(%s)" % tablename)
    pragma = c.fetchall()
    # Create a copy of the pragma table
    header = u'cid integer primary key, name text, type text, notn integer, dflt text, pk integer'
    c.execute(u'CREATE TABLE %s (%s)' % (pragmaname, header))
    # The 6th column is 1 if the column is a primary key, 0 if not.
    # Scan the results for a primary key.
    # Now figure out what kind of container each column needs
    for row in pragma:
        rowdata = [data for data in row]
        if row['pk'] == 0:
            cmd = u"Select %s from %s" % (row['name'],tablename)
            results = [unicode(item[0]) for item in c.execute(cmd)]
            rowdata[2] = find_list_type(results)
        c.execute(u'INSERT INTO %s VALUES (%s)' % (pragmaname, sql_list(rowdata)))

def change_column_type(conn, tablename, colname, newtype, pragmaname=u'temp_pragma'):
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute(u"""UPDATE %s SET type="%s" WHERE name="%s" """ % (pragmaname, newtype, colname))

def create_table(conn, tablename, pragmaname='temp_pragma'):
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    # Get info from pragma
    c.execute("select pk, name || ' ' || type as nametype from temp_pragma")
    pragma = c.fetchall()
    headerlist = []
    for row in pragma:
        if row['pk'] == 1:
            headerlist.append(row['nametype'] + ' primary key')
        else:
            headerlist.append(row['nametype'])
    header = u', '.join(headerlist)
    cmd = u'CREATE TABLE %s (%s)' % (tablename, header,)
    c.execute(cmd)

def copy_table(conn, old_table_name, new_table_name):
    conn.row_factory = sqlite3.Row
    c = conn.cursor()
    c.execute('SELECT * FROM %s' % (old_table_name,))
    for row in c.fetchall():
        values = sql_list([item for item in row])
        keys = sql_list(row.keys())
        cmd = "INSERT INTO %s (%s) values (%s)" % (new_table_name, keys, values)
        c.execute(cmd)

if __name__ == '__main__':
    conn = sqlite3.connect(':memory:')
    c = conn.cursor()

    temp_table_name = 'temp_data'
    tablename = 'data'

    with open('data.csv') as fin:
        s = fin.read()
        import_csv_str_to_db(s,';',temp_table_name,conn)

    detect_column_types(conn, temp_table_name)

    # Change the type of the 'BILL_RUN_DATE' column to text:
    change_column_type(conn, 'data', 'BILL_RUN_DATE', u'text')
    
    create_table(conn, tablename)

    # Now copy all the old data to the new table
    copy_table(conn, temp_table_name, tablename)
    # # Now try changing the format of the 'BILL_RUN_DATE' column
    num_records = c.execute('SELECT count(id) FROM data').fetchone()[0]

    for i in range(1, num_records + 1):
        value = c.execute('select bill_run_date from data where id = %s' % (str(i),)).fetchone()[0]
        value = datetime.date(int(value[0:4]), int(value[4:6]),int(value[6:8])).isoformat()
        c.execute("UPDATE data SET BILL_RUN_DATE='%s' WHERE id=%s" % (value,str(i)))



