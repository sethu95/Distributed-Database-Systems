#
# Assignment2 Interface
#

import psycopg2
import os
import sys
import threading


# Receive as min, max = getMinAndMax('ratings', 'rating', cur)
def getMinAndMax(InputTable, SortingColumnName, cur):
    minmaxquery = "SELECT MAX(" + SortingColumnName + "), MIN(" + SortingColumnName + ") FROM " + InputTable + ";"
    cur.execute(minmaxquery)
    return cur.fetchone()

# Receive as cols = getColumnNamesFromTable('ratings', cur), output will be columnname, datatype list
def getColumnNamesFromTable(InputTableName, cur):
    columnsql = "SELECT column_name, data_type FROM information_schema.columns WHERE table_name = \'" + InputTableName + "\'";
    cur.execute(columnsql)
    return cur.fetchall()

# Receive as cols = getColumnNamesFromTable('ratings', cur), output will be columnname, datatype list
def dropAndCreateTable(InputTable, OutputTable, cur):
    dropsql = "DROP TABLE IF EXISTS " + OutputTable + ";"
    cur.execute(dropsql)
    createsql = "CREATE TABLE IF NOT EXISTS " + OutputTable + " ( LIKE " + InputTable + " INCLUDING ALL);"
    cur.execute(createsql)

def dropAndCreateTableJoin(cols1, cols2 , OutputTable, con):
    cur = con.cursor()
    dropsql = "DROP TABLE IF EXISTS " + OutputTable + ";"
    cur.execute(dropsql)
    col = ""
    for i in (cols1):
        col = col + i[0] + " " + i[1] + ","
    
    for i in (cols2):
        col = col + i[0] + " " + i[1] + ","
    
    createsql = "CREATE TABLE " + OutputTable + " ("+ col[0:len(col)-1] +");"
    cur.execute(createsql)
    con.commit()

def insertIntoMainTable(OutputTable, TEMP_PREFIX, threadMonitor, cur):
    for index in range(len(threadMonitor)):
        TempTableName = TEMP_PREFIX + str(index)
        insertsql = "INSERT INTO " +OutputTable+ " SELECT * FROM " +TempTableName+";"
        threadMonitor[index].join()
        cur.execute(insertsql)
    
def sortingFunction(InputTable, SortingColumnName, TempTableName, openconnection, lowerbound, upperbound, index, OutputTable):
    con = openconnection
    cur = con.cursor()
    dropAndCreateTable(InputTable, TempTableName, cur)
    if index==0:
        sortsql = "INSERT INTO " + TempTableName + " SELECT * FROM " + InputTable + " WHERE " +SortingColumnName+ " >= " +str(lowerbound)+ " AND " +SortingColumnName+ " <= " +str(upperbound)+ " ORDER BY " +SortingColumnName+ " ASC;"
    else:
        sortsql = "INSERT INTO " + TempTableName + " SELECT * FROM " + InputTable + " WHERE " +SortingColumnName+ " > " +str(lowerbound)+ " AND " +SortingColumnName+ " <= " +str(upperbound)+ " ORDER BY " +SortingColumnName+ " ASC;"
    cur.execute(sortsql)
#     insertsql = "INSERT INTO " +OutputTable+ " SELECT * FROM " +TempTableName+";"
#     cur.execute(insertsql)
    cur.close()
    con.commit()

def joiningFunction(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, TempTableName, con, lowerbound1, upperbound1, cols1, cols2, index, OutputTable):
    cur = con.cursor()
    dropAndCreateTableJoin(cols1, cols2 , TempTableName, con)
    if index==0:
        joinsql = "INSERT INTO " + TempTableName + " SELECT * FROM " + InputTable1 + " T1 INNER JOIN  " + InputTable2 + " T2 ON T1." + Table1JoinColumn + " = T2." + Table2JoinColumn + " AND T1."+ Table1JoinColumn  +" >= " +str(lowerbound1)+ " AND T1." + Table1JoinColumn   + " <= " +str(upperbound1)+ " ORDER BY " + Table1JoinColumn + " ASC;"
    else:
        joinsql = "INSERT INTO " + TempTableName + " SELECT * FROM " + InputTable1 + " T1 INNER JOIN  " + InputTable2 + " T2 ON T1." + Table1JoinColumn + " = T2." + Table2JoinColumn + " AND T1."+ Table1JoinColumn  +" > " +str(lowerbound1)+ " AND T1." + Table1JoinColumn   + " <= " +str(upperbound1)+ " ORDER BY " + Table1JoinColumn + " ASC;"
    cur.execute(joinsql)
#     insertsql = "INSERT INTO " +OutputTable+ " SELECT * FROM " +TempTableName+";"
#     cur.execute(insertsql)
    cur.close()
    con.commit()

def deleteTempTables(TEMP_PREFIX, threads, con):
    for index in range(threads):
        TempTableName = TEMP_PREFIX + str(index)
        deleteTables(TempTableName, con)

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    #Implement ParallelSort Here.
    con = openconnection
    cur = con.cursor()

    max, min = getMinAndMax(InputTable, SortingColumnName, cur)
    threads = 5
    steprange = (max-min)/threads
    
    dropAndCreateTable(InputTable, OutputTable, cur)
    
    threadMonitor = [0] * threads
    TEMP_PREFIX = "temp_dds_asgmt2_sort"
    for index in range(threads):
        lowerbound = min + index*steprange
        upperbound = min + (index+1)*steprange
        TempTableName = TEMP_PREFIX + str(index)
        threadMonitor[index] = threading.Thread(target=sortingFunction, args=(InputTable, SortingColumnName, TempTableName, con, lowerbound, upperbound, index, OutputTable))
        threadMonitor[index].start()
    insertIntoMainTable(OutputTable, TEMP_PREFIX, threadMonitor,  cur)
    deleteTempTables(TEMP_PREFIX, threads, con)
    cur.close()
    con.commit()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    #Implement ParallelJoin Here.
    con = openconnection
    cur = con.cursor()

    max1, min1 = getMinAndMax(InputTable1, Table1JoinColumn, cur)
    max2, min2 = getMinAndMax(InputTable2, Table2JoinColumn, cur)
    
    threads = 5
    steprange1 = (max1-min1)/threads
    steprange2 = (max2-min2)/threads
    
    cols1 = getColumnNamesFromTable(InputTable1, cur)
    cols2 = getColumnNamesFromTable(InputTable2, cur)
    dropAndCreateTableJoin(cols1, cols2 , OutputTable, con)
    threadMonitor = [0] * threads
    TEMP_PREFIX = "temp_dds_asgmt2_join"
    for index in range(threads):
        lowerbound1 = min1 + index*steprange1
        upperbound1 = min1 + (index+1)*steprange1
        TempTableName = TEMP_PREFIX + str(index)
        threadMonitor[index] = threading.Thread(target=joiningFunction, args=(InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, TempTableName, con, lowerbound1, upperbound1, cols1, cols2, index, OutputTable))
        threadMonitor[index].start()
    insertIntoMainTable(OutputTable, TEMP_PREFIX, threadMonitor,  cur)
    deleteTempTables(TEMP_PREFIX, threads, con)
    cur.close()
    con.commit()




################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='dds_assignment2'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='dds_assignment2'):
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
    con.commit()
    con.close()

# Donot change this function
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
        sys.exit(1)
    except IOError as e:
        if openconnection:
            openconnection.rollback()
        print('Error %s' % e)
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()
