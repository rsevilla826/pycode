''' Author: Robert Sevilla
    Date: 9/12/2017
    Description: This script will either execute all dot run_ sql files from a directory or
                 take -f <sql_file_name>, as an argument to execute a single sql script. If you would like
                 to cycle through and execute all files in the SQL directory, pass option -d. If you pass -f 
                 <sql_file_name>
'''

import re
import os
import getopt
import csv
import zipfile
import zlib
import sys, traceback
from os import walk

from SnowConnections import SnowConnections
sc = SnowConnections()
CONNECTION_STRING = 'SnowFlakeDB'

HELP_MSG = 'sf_util.py [-f <filename>] -d to cycle through all run_ SQL files in a directory and execute them. Pass -e to dump from a SQL select and -z to zip'

def run_sql_directory(sql_dir,fnames,conn):
    cs = conn.cursor()
    for files in fnames:
        if re.match(r'run_.*sql',files):
            sf_sql_file = sql_dir + '/' + files
            with open(sf_sql_file, 'r') as sql_file:
                data = sql_file.read()
                print('**** Running SQL file ' + sf_sql_file)
                try:
                    cs.execute(data)
                except snowflake.connector.errors.ProgrammingError as e:
                    print(e)
                    print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
                finally:
                    cs.close()

def run_sql_file(sql_dir,fname,conn):
    cs = conn.cursor()
    sf_sql_file = sql_dir + '/' + fname
    with open(sf_sql_file, 'r') as sql_file:
        data = sql_file.read()
        print('**** Running SQL file ' + sf_sql_file)
        try:
            cs.execute(data)
        except snowflake.connector.errors.ProgrammingError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
        finally:
            cs.close()

def dump_sql(sql_dir,fname,conn):
    cs = conn.cursor()
    sf_sql_file = sql_dir + '/' + fname
    with open(sf_sql_file, 'r') as sql_file:
        data = sql_file.read()
        try:
            cs.execute(data)
            return cs
        except snowflake.connector.errors.ProgrammingError as e:
            print(e)
            print('Error {0} ({1}): {2} ({3})'.format(e.errno, e.sqlstate, e.msg, e.sfqid))
    close_cursor(cs)

def close_cursor(curr):
    curr.close()

def create_csv_file(cursor,csv_file_out):
    file_path = csv_file_out
    with open(file_path, "w", encoding='utf-8') as csv_file:
        csv_writer = csv.writer(csv_file)
        csv_writer.writerow([i[0] for i in cursor.description]) # write headers
        csv_writer.writerows(cursor)
    print("The following export file has been created: " + file_path)

def zip_this(csv_file_path,zip_file,export_path):
    zip_file_path = file_path = zip_file
    zf = zipfile.ZipFile(zip_file_path, mode='w')
    zf.write(csv_file_path, export_path, zipfile.ZIP_DEFLATED)
    zf.close()
    os.remove(csv_file_path)

def get_file_names(sql_dir):
    f = []
    for (dirpath, dirnames, filenames) in walk(sql_dir):
        f.extend(filenames)
        break
    return f

if __name__ == '__main__':
    sql_dir = '<SQL_DIR_REPLACE_ME>'
    export_dir = '<EXPORT_DIR_REPLACE_ME'

    files = True
    file_dir = False
    export_sql = False
    zip_me = False

    try:
        opts, remainder = getopt.gnu_getopt(sys.argv[1:], "f:c:o:x:dez", ["filename=","CONNECTION_STRING=","sql_dir_overwrite=","exp_dir_overwrite="])
    except getopt.GetoptError:
        print(HELP_MSG)
        sys.exit(2)

    for opt, arg in opts:
        if opt in ('-f', '--filename'):
            filename = arg
            files = True
        elif opt in ('-c', '--CONNECTION_STRING'):
            CONNECTION_STRING = arg
        elif opt in ('-o', '--sql_dir_overwrite'):
            sql_dir = arg
        elif opt in ('-x', '--exp_dir_overwrite'):
            export_dir = arg
        elif opt in ('-d'):
            file_dir = True
        elif opt in ('-e'):
            export_sql = True
        elif opt in ('-z'):
            zip_me = True

    # Open SnowFlake connection
    conn = sc.openConnection(CONNECTION_STRING)

    # Check command flags
    if file_dir:
        fnames = get_file_names(sql_dir)
        run_sql_directory(sql_dir,fnames,conn)
    elif files and not export_sql:
        run_sql_file(sql_dir,filename,conn)
    elif export_sql:
        split_fname = filename.split('.')
        csv_file = "{0}/{1}.csv".format(export_dir,split_fname[0])
        cursor = dump_sql(sql_dir,filename,conn)
        create_csv_file(cursor,csv_file)

    if zip_me:
        zip_file = "{0}/{1}.zip".format(export_dir,split_fname[0])
        zip_this(csv_file,zip_file,export_dir)
