#!/usr/bin/python
# -*- coding: utf-8 -*-

import ConfigParser
import os
import sys
from datetime import datetime, timedelta

import psycopg2

cfg_file = "/root/newjasper/conf/ewsd.conf"

### Read config file

# config = ConfigParser.RawConfigParser(allow_no_value=True)
config = ConfigParser.RawConfigParser()
config.read(cfg_file)

if '-t' in sys.argv:
    print("Run test ewsd2pg.")
    cfg_file = "/root/newjasper/conf/ewsd.conf"
    status = "copy"
elif '-c' in sys.argv:
    print("Run create table " + config.get("pg", "table") + ".")
    status = "create"
elif '-d' in sys.argv:
    print("Run del table" + config.get("pg", "table") + ".")
    status = "drop"
elif '-e' in sys.argv:
    print("Run erase data from table" + config.get("pg", "table") + ".")
    status = "erase"
elif '-i' in sys.argv:
    print("Run create index from table" + config.get("pg", "table") + ".")
    status = "index"
elif '-p' in sys.argv:
    print("Creat partition table" + config.get("pg", "table") + ".")
    status = "partition"
else:
    print("It is test. Run ewsd2pg.py -t .Exit")
    status = "null"
    quit()

### Connect to postgres DB

conn = psycopg2.connect(
    database=config.get("pg", "db"),
    user=config.get("pg", "user"),
    password=config.get("pg", "passwd"),
    host=config.get("pg", "host"),
    port=config.get("pg", "port")
)

print("Database Connected....")

ddd = {}
cur = conn.cursor()


###  Manager table
def _manager_table(status_exe):
    if status_exe == "create":
        qlncreate = "CREATE SEQUENCE logid START 1; CREATE TABLE " + config.get("pg", "table") + "(id integer PRIMARY KEY DEFAULT nextval('logid'),"
        for k in [100, 101, 102, 105, 106, 110, 119, 120, 130, 134, 142, 153, 157, 159, 168, 170, 171, 172, 201, 202, 203, 204, 205]:
            qlncreate += config.get("ewsd-field", "type" + str(k)) + " " + config.get("ewsd-val", "type" + str(k)) + " ,"
        qlncreate = qlncreate[:-2] + "); "
        cur.execute(qlncreate)

        print("Table " + config.get("pg", "table") + " Created.")

    elif status_exe == "partition":
        qlndrop = "SELECT create_range_partitions('ewsd.log', 'datepacket', '2017-01-01'::date, '1 day'::interval);"
        cur.execute(qlndrop)
        print("Table " + config.get("pg", "table") + " create partition.")

    elif status_exe == "index":
        qlndrop = "CREATE INDEX i_ewsd_log ON ewsd.log(datepacket);"
        cur.execute(qlndrop)
        print("Table " + config.get("pg", "table") + " create partition.")

    elif status_exe == "drop":
        qlndrop = "DROP TABLE IF EXISTS " + config.get("pg", "table") + " CASCADE ; DROP SEQUENCE logid;"
        cur.execute(qlndrop)
        print("Table " + config.get("pg", "table") + " Deleted.")

    elif status_exe == "erase":
        qlndrop = "DELETE FROM " + config.get("pg", "table") + ";"
        cur.execute(qlndrop)
        print("Table " + config.get("pg", "table") + " Erase.")

    elif status_exe == "copy":
        columname = ""
        for k in [100, 101, 102, 105, 106, 110, 119, 120, 130, 134, 142, 153, 157, 159, 168, 170, 171, 172, 201, 202,
                  203, 204, 205]:
            columname += config.get("ewsd-field", "type" + str(k)) + " ,"
        columname = columname[:-2]

        qlncopy = "COPY " + config.get("pg", "table") + "(" + columname + ") FROM STDIN DELIMITER ',' CSV"
        with open(config.get("file", "work-dir") + config.get("file", "data-dir") + config.get("file", "tmp-file"),
                  'r') as fi:
            cur.copy_expert(qlncopy, fi)
        print("Copy " + config.get("pg", "table") + " Ok.")
        fi.close()
        conn.commit()
        open(config.get("file", "work-dir") + config.get("file", "data-dir") + config.get("file", "tmp-file"),
             'w').close()

    else:
        print("Nothing to do!")

    conn.commit()


### Clear variables
def _clear_var():
    for k in [100, 101, 102, 105, 106, 110, 119, 120, 130, 134, 142, 153, 157, 159, 168, 170, 171, 172, 201, 202, 203,
              204, 205]:
        ddd[config.get("ewsd-field", "type" + str(k))] = ""
    return ()


### Parse file ewsd.log
def _file_parse(file_name):
    open(config.get("file", "work-dir") + config.get("file", "data-dir") + config.get("file", "tmp-file"), 'w').close()
    tf = open(config.get("file", "work-dir") + config.get("file", "data-dir") + config.get("file", "tmp-file"), 'a')
    f = open(config.get("file", "work-dir") + config.get("file", "data-dir") + file_name, 'r')

    for line in f:
        _clear_var()
        for n in line.split("; "):
            for k in [101, 102, 110, 119, 120, 130, 134, 142, 153, 157, 159, 168, 170, 171, 172]:
                if (config.get("ewsd-field", "type" + str(k))) in n:
                    ddd[config.get("ewsd-field", "type" + str(k))] = n.replace(",", "").split(" ")[3]

            if (config.get("ewsd-field", "type100")) in n:
                ddd[config.get("ewsd-field", "type100")] = n.replace(",", "").split(" ")[4] + " " + \
                                                           n.replace(",", "").split(" ")[5]
                ddd[config.get("ewsd-field", "type201")] = n.replace(",", "").split(" ")[7]
                ddd[config.get("ewsd-field", "type202")] = str(
                    datetime.strptime(ddd[config.get("ewsd-field", "type100")], "%Y-%m-%d %H:%M:%S") + timedelta(
                        seconds=int(ddd[config.get("ewsd-field", "type201")])))
            elif config.get("ewsd-field", "type105") in n:
                ddd[config.get("ewsd-field", "type105")] = n.split(" ")[3].split(":")[0]
                ddd[config.get("ewsd-field", "type203")] = n.split(" ")[3].split(":")[1]
            elif config.get("ewsd-field", "type106") in n:
                ddd[config.get("ewsd-field", "type106")] = n.split(" ")[3].split(":")[0]
                ddd[config.get("ewsd-field", "type204")] = n.split(" ")[3].split(":")[1]

        ln = ""
        ddd[config.get("ewsd-field", "type205")] = file_name
        if ddd[config.get("ewsd-field", "type100")] != "":
            for kk in [100, 101, 102, 105, 106, 110, 119, 120, 130, 134, 142, 153, 157, 159, 168, 170, 171, 172, 201,
                       202, 203, 204, 205]:
                ln += ddd[config.get("ewsd-field", "type" + str(kk))] + ","
            ln = ln[:-1] + "\n"

        tf.write(ln)

    f.close()
    tf.close()


### List file inserted db
def _list_db_f():
#    query  = "SELECT filename FROM "+config.get("pg", "table")+" where "+config.get("ewsd-field", "type100")+" > now() - interval '"+config.get("file", "interval")+" hour' group by 1;"
    query = "SELECT filename FROM " + config.get("pg", "table") + " group by 1;"
    print(query)
    cur.execute(query)
    data = cur.fetchall()
    list_add_file = []

    for rec in data:
        list_add_file.append(str(rec[0]).split()[0])
    return list_add_file


### List files find in data dir.
def _list_l(directory):
    list_file = []
    list_dir = os.listdir(directory)
    for file_dir in list_dir:
        if file_dir.startswith(config.get("server", "prefix")):
            list_file.append(file_dir)
    return list_file


### Test file in db or data dir
def _data_diff():
    lf_db = _list_db_f()
    for filename in _list_l(config.get("file", "work-dir") + config.get("file", "data-dir")):
        if not filename in lf_db:
            print("New file: " + filename)
            _file_parse(filename)
            _manager_table(status)
            print("File " + filename + " inserted OK.")
        else:
            print("File in db: " + filename)


if status == "copy":
    _data_diff()
else:
    _manager_table(status)

print("END")

########################
#     CREATE SEQUENCE logid START 1;
#     CREATE TABLE bar (id integer PRIMARY KEY DEFAULT nextval('logid'));
#     CREATE INDEX i_ewsd_log ON ewsd.log(datepacket);
#     SELECT create_range_partitions('ewsd.log', 'datepacket', '2017-01-01'::date, '1 day'::interval);
########################
