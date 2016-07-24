#!/usr/bin/env python
# coding: utf-8

# (setq python-shell-interpreter "ipython")
"""csv to hive"""

import pandas as pd

def csv2hql(csvfile, hqlfile=None, db="tmp_sparkdb"):
    """
    Generate HQL file to load csv into hive table
    """
    df = pd.read_csv(csvfile)
    table = csvfile.split(".")[-2].split("/")[-1]  # infer table name from file name
    hqlfile = table + ".hql"

    sql_query = list()
    sql_query.append("-- CREATE DATABASE IF NOT EXISTS %s" % (db))
    sql_query.append("USE %s" % (db))
    sql_query.append("DROP TABLE IF EXISTS %s.%s" % (db, table))
    create_table_query = """CREATE TABLE IF NOT EXISTS %s.%s (\n""" % (db, table)
    for column in df.columns[:-1]:
        column = column.replace('"', '').replace(" ", "")
        create_table_query += """`%s` STRING COMMENT "%s",\n""" % (column, column)
    create_table_query += """`%s` STRING COMMENT "%s"\n""" % (df.columns[-1], df.columns[-1])
    create_table_query += """)\nROW FORMAT DELIMITED FIELDS TERMINATED BY "," ESCAPED BY "\\\\"  LINES TERMINATED BY "\\n"\nSTORED AS TEXTFILE"""
    sql_query.append(create_table_query)
    sql_query.append("LOAD DATA LOCAL INPATH '%s' OVERWRITE INTO TABLE %s.%s" % (csvfile, db, table))

    f = open(hqlfile, 'w')
    f.write(str(";\n").join(sql_query))
    f.write(";\n")
    f.close()


if __name__ == "__main__":
    db = "tmp_sparkdb"
    csvfile = "../data/test.csv"
    csv2hql(csvfile, db=db)
