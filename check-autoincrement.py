#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
Check if mysql auto_increment columns are close to reaching overflow
"""
import sys

import MySQLdb

connection = {
    'dbhost'      : '127.0.0.1',
    'dbuser'      : 'root',
    'critical'    : 0.85,
    'warning'     : 0.70,
    'verbosity'   : 0
}

max_size = {
    'unsigned_bigint'    : 18446744073709551615,
    'unsigned_int'       : 4294967295,
    'unsigned_integer'   : 4294967295,
    'unsigned_smallint'  : 65535,
    'unsigned_tinyint'   : 255,
    'unsigned_mediumint' : 16777215,
    'signed_bigint'      : 9223372036854775807,
    'signed_int'         : 2147483647,
    'signed_integer'     : 2147483647,
    'signed_smallint'    : 32767,
    'signed_tinyint'     : 127,
    'signed_mediumint'   : 8388607
}

has_warnings = 0
has_critical = 0
db=''
max_record = {
    'database'      : '',
    'table'         : '',
    'column'        : '',
    'fill'          : 0,
    'value'         : 0,
    'max'           : 0
}

def connect_to_sql(connection):
print ('Connecting with user: {}'.format(connection['dbuser']))
connect_args = dict(
host=connection.get('dbhost', '127.0.0.1'),
user=connection['dbuser'],
        password=connection.get('password'))

conn = MySQLdb.connect(**connect_args)
return conn, conn.cursor()

def display_warning(ret):
    for val in ret:
        catalog, database, table, column, type, auto_increment, column_type = val.split()
        if (auto_increment):
            type_with_signed = ('unsigned_'+type) if 'unsigned' in column_type else ('signed_'+type)
            max=max_size[type_with_signed]
            if not max:
                if connection['verbosity'] >= 1:
                    print ("Don't know maximal value for data type {} ".format(type_with_signed))

            fill = auto_increment / max

            if connection['verbosity'] >= 2:
                print ('{}  {}  {}  {}  {}  {}  {} '.format(catalog, database, table, column, type_with_signed, auto_increment, fill))

            if fill >= connection['critical']:
                has_critical = has_critical + 1
                print ('CRITICAL: {}.{}.{} at {:.3f} ({}/{})'.format(database, table, column, fill, auto_increment, max))

            if fill >= connection['warning']:
                has_warnings = has_warnings + 1
                print ('WARNING: {}.{}.{} at {:.3f} ({}/{})'.format(database, table, column, fill, auto_increment, max))

            if (not max_record or max_record['fill'] <= fill):
                max_record = {
                    'database'      : database,
                    'table'         : table,
                    'column'        : column,
                    'fill'          : fill,
                    'value'         : auto_increment,
                    'max'           : max
                }

    if not has_warnings and not has_critical and max_record:
        print ('OK (maximal value:  : {}.{}.{} at {:.3f} ({}/{})'.format(max_record['database'],max_record['table'],max_record['column'],max_record['fill'],max_record['value'],max_record['max']))

    if has_critical:
        sys.exit(1)
    elif has_warnings:
        sys.exit(1)
    else:
        sys.exit(0)


if __name__ == '__main__':
    """
    Entry point
    """
    conn, cursor = connect_to_sql(connection)
    query = """
    SELECT c.table_catalog, c.table_schema, c.table_name, c.column_name, c.data_type, t.auto_increment, c.column_type
    FROM information_schema.columns AS c
    JOIN information_schema.tables  AS t
         ON c.table_catalog = t.table_catalog
        AND c.table_schema  = t.table_schema
        AND c.table_name    = t.table_name
    WHERE c.extra LIKE '%auto_increment%'
    ORDER BY c.table_catalog, c.table_schema, c.table_name, c.column_name
    """
    cursor.execute(query)
    ret = cursor.fetchall()
    display_warning(ret)
