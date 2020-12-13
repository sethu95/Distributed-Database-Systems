import psycopg2
import os
import sys
import numpy as np


def getOpenConnection(user='postgres', password='1234', dbname='postgres'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def loadRatings(ratingstablename, ratingsfilepath, openconnection):
    con = openconnection
    cur = con.cursor()
    createsql = "CREATE TABLE " + ratingstablename + " (userid INT, emptcol1 VARCHAR, movieid INT, emptcol2 VARCHAR, rating FLOAT, emptcol3 VARCHAR, date BIGINT);"
    altersql = "ALTER TABLE " + ratingstablename + " DROP COLUMN date, DROP COLUMN  emptcol1, DROP COLUMN  emptcol2, DROP COLUMN  emptcol3"
    try:
        cur.execute(createsql)
    except Exception as e:
        print(e)
    try:
        cur.copy_from(open(ratingsfilepath),ratingstablename,sep=':')
    except Exception as e:
        print(e)
    cur.execute(altersql);
    cur.close()
    con.commit()


def rangePartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    rangevalue = 5 / numberofpartitions
    PARTITION_TABLE = "range_ratings_part"
    
    for i in range(0, numberofpartitions):
        low = i*rangevalue
        high = (i+1)*rangevalue
        table = PARTITION_TABLE + str(i)
        createsql = "CREATE TABLE " + table + " (userid INT, movieid INT, rating FLOAT);"
        cur.execute(createsql)
        
        if i==0:
            cur.execute("INSERT INTO " + table + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating>=" + str(low) + " AND rating<=" + str(high) + ";")
        else:
            cur.execute("INSERT INTO " + table + " (userid, movieid, rating) SELECT userid, movieid, rating FROM " + ratingstablename + " WHERE rating>" + str(low) + " AND rating<=" + str(high) + ";")
    cur.close()
    con.commit()

def roundRobinPartition(ratingstablename, numberofpartitions, openconnection):
    con = openconnection
    cur = con.cursor()
    PARTITION_TABLE = "round_robin_ratings_part"
    
    for i in range(0, numberofpartitions):
        table = PARTITION_TABLE + str(i)
        createsql = "CREATE TABLE " + table + " (userid INT, movieid INT, rating FLOAT);"
        cur.execute(createsql)
        cur.execute("INSERT INTO " + table + " (userid, movieid, rating) SELECT userid, movieid, rating FROM (SELECT userid, movieid, rating, ROW_NUMBER() over() as rnum FROM "+ ratingstablename +") AS subtable WHERE MOD(subtable.rnum-1,"+str(numberofpartitions)+") = "  + str(i) + ";")
    cur.close()
    con.commit()


def roundRobinInsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    PARTITION_TABLE = "round_robin_ratings_part"
    
    countrowssql = "SELECT COUNT(*) FROM " +ratingstablename+ ";"
    numberofpartitionssql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '%"+ PARTITION_TABLE +"%'"
    insertmaintable = "INSERT INTO " + ratingstablename + " VALUES (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");"
    cur.execute(insertmaintable)
    cur.execute(countrowssql)
    countrows = (cur.fetchall())[0][0]
    cur.execute(numberofpartitionssql)
    numberofpartitions = (cur.fetchall())[0][0]
    
    insertid = (countrows-1)%(numberofpartitions)
    partitiontable = PARTITION_TABLE + str(insertid)
    
    insertparttable = "INSERT INTO " + partitiontable + " VALUES (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");"
    cur.execute(insertparttable)
    
    cur.close()
    con.commit()
    

def rangeInsert(ratingstablename, userid, itemid, rating, openconnection):
    con = openconnection
    cur = con.cursor()
    PARTITION_TABLE = "range_ratings_part"
    
    numberofpartitionssql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '%"+ PARTITION_TABLE +"%'"
    cur.execute(numberofpartitionssql)
    numberofpartitions = (cur.fetchall())[0][0]
    rangevalue = 5/numberofpartitions
    
    for i in range(0, numberofpartitions-1):
        low = i*rangevalue
        high = (i+1)*rangevalue
        if(i==0):
            if(rating >= low and rating <=high):
                insertid = i
                break
        else:
            if(rating > low and rating <=high):
                insertid = i
                break
    partitiontable = PARTITION_TABLE + str(insertid)
    insertmaintable = "INSERT INTO " + ratingstablename + " VALUES (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");"
    insertparttable = "INSERT INTO " + partitiontable + " VALUES (" + str(userid) + ", " + str(itemid) + ", " + str(rating) + ");"
    cur.execute(insertmaintable)
    cur.execute(insertparttable)
    
    cur.close()
    con.commit()

def rangeQuery(ratingMinValue, ratingMaxValue, openconnection, outputPath):
    con = openconnection
    cur = con.cursor()
    rangeprefix = "range_ratings_part"
    
    numberofpartitionssql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '%"+ rangeprefix +"%'"
    cur.execute(numberofpartitionssql)
    numberofpartitions = (cur.fetchall())[0][0]
    with open(outputPath, "a") as file:
        file.truncate(0)
        for i in range(0, numberofpartitions):
            table = rangeprefix + str(i)
            selectsql = "SELECT * FROM " + table + " WHERE rating>=" + str(ratingMinValue) + " AND rating<=" + str(ratingMaxValue) + ";"
            cur.execute(selectsql)
            allrows = cur.fetchall()
            for row in allrows:
                file.write(table+","+str(row[0])+","+str(row[1])+","+str(row[2])+"\n")
        file.close()
    
    
    rrobinprefix = "round_robin_ratings_part"
    
    numberofpartitionssql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '%"+ rrobinprefix +"%'"
    cur.execute(numberofpartitionssql)
    numberofpartitions = (cur.fetchall())[0][0]
    with open(outputPath, "a") as file:
        for i in range(0, numberofpartitions):
            table = rrobinprefix + str(i)
            selectsql = "SELECT * FROM " + table + " WHERE rating>=" + str(ratingMinValue) + " AND rating<=" + str(ratingMaxValue) + ";"
            cur.execute(selectsql)
            allrows = cur.fetchall()
            for row in allrows:
                file.write(table+","+str(row[0])+","+str(row[1])+","+str(row[2])+"\n")
    file.close()
    
        


def pointQuery(ratingValue, openconnection, outputPath):
    con = openconnection
    cur = con.cursor()
    rangeprefix = "range_ratings_part"
    
    numberofpartitionssql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '%"+ rangeprefix +"%'"
    cur.execute(numberofpartitionssql)
    numberofpartitions = (cur.fetchall())[0][0]
    with open(outputPath, "a") as file:
        for i in range(0, numberofpartitions):
            file.truncate(0)
            table = rangeprefix + str(i)
            selectsql = "SELECT * FROM " + table + " WHERE rating =" + str(ratingValue) + ";"
            cur.execute(selectsql)
            allrows = cur.fetchall()
            for row in allrows:
                file.write(table+","+str(row[0])+","+str(row[1])+","+str(row[2])+"\n")
    file.close()
    
    
    rrobinprefix = "round_robin_ratings_part"
    
    numberofpartitionssql = "SELECT COUNT(*) FROM information_schema.tables WHERE table_name LIKE '%"+ rrobinprefix +"%'"
    cur.execute(numberofpartitionssql)
    numberofpartitions = (cur.fetchall())[0][0]
    with open(outputPath, "a") as file:
        for i in range(0, numberofpartitions):
            table = rrobinprefix + str(i)
            selectsql = "SELECT * FROM " + table + " WHERE rating =" + str(ratingValue) + ";"
            cur.execute(selectsql)
            allrows = cur.fetchall()
            for row in allrows:
                file.write(table+","+str(row[0])+","+str(row[1])+","+str(row[2])+"\n")
    file.close()


def createDB(dbname='dds_assignment1'):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getOpenConnection(dbname='postgres')
    con.set_isolation_level(psycopg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
    cur = con.cursor()

    # Check if an existing database with the same name exists
    cur.execute('SELECT COUNT(*) FROM pg_catalog.pg_database WHERE datname=\'%s\'' % (dbname,))
    count = cur.fetchone()[0]
    if count == 0:
        cur.execute('CREATE DATABASE %s' % (dbname,))  # Create the database
    else:
        print('A database named {0} already exists'.format(dbname))

    # Clean up
    cur.close()
    con.close()

def deleteTables(ratingstablename, openconnection):
    try:
        cursor = openconnection.cursor()
        if ratingstablename.upper() == 'ALL':
            cursor.execute("SELECT table_name FROM information_schema.tables WHERE table_schema = 'public'")
            tables = cursor.fetchall()
            for table_name in tables:
                cursor.execute('DROP TABLE %s CASCADE' % (table_name[0]))
        else:
            cursor.execute('DROP TABLE %s CASCADE' % (ratingstablename))
        openconnection.commit()
    except psycopg2.DatabaseError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
    finally:
        if cursor:
            cursor.close()
