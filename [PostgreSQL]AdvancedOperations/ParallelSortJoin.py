#!/usr/bin/env python2.7
import psycopg2
import threading
import os
import sys

##################### This needs to changed based on what kind of table we want to sort. ##################
##################### To know how to change this, see Assignment 3 Instructions carefully #################
FIRST_TABLE_NAME = 'ratings'
SECOND_TABLE_NAME = 'movies'
SORT_COLUMN_NAME_FIRST_TABLE = 'movieid'
SORT_COLUMN_NAME_SECOND_TABLE = 'movieid1'
JOIN_COLUMN_NAME_FIRST_TABLE = 'movieid'
JOIN_COLUMN_NAME_SECOND_TABLE = 'movieid1'
THREAD_NUM = 5
##########################################################################################################

def deleteOneTable(ratingstablename, openconnection):
    cur = openconnection.cursor()
    cur.execute("DROP TABLE IF EXISTS "+ratingstablename)
    cur.close()
    openconnection.commit()

# Donot close the connection inside this file i.e. do not perform openconnection.close()
def ParallelSort (InputTable, SortingColumnName, OutputTable, openconnection):
    cursor = openconnection.cursor()

    def worker(num, t):
        if(t): t.join()
        cmd = "INSERT INTO {4}  SELECT * FROM {0} WHERE {1} >= {2} and {1} < {3} ORDER by {1} ASC".format(InputTable, SortingColumnName, MIN_KEY + MAX_KEY*((num-1)/5.0), MIN_KEY + (MAX_KEY)*(num/5.0), OutputTable)
        print cmd
        cursor.execute(cmd)


    threads = (THREAD_NUM+1)*[None]
    cursor.execute("SELECT MAX({0}) FROM {1}".format(SortingColumnName, InputTable))
    MAX_KEY = cursor.fetchone()[0] + 1
    cursor.execute("SELECT MIN({0}) FROM {1}".format(SortingColumnName, InputTable))
    MIN_KEY = cursor.fetchone()[0]

    cursor.execute("DROP TABLE IF EXISTS "+OutputTable)
    cmd = "CREATE TABLE {0} AS SELECT * FROM {1} WHERE 1=2".format(OutputTable, InputTable)
    print cmd
    cursor.execute(cmd)
    for i in xrange(1, THREAD_NUM+1):
        t = threading.Thread(target=worker, args=(i, threads[i-1]))
        threads[i] = t
        t.start()

    threads[-1].join()


    cursor.close()
    openconnection.commit()

def ParallelJoin (InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, OutputTable, openconnection):
    cursor = openconnection.cursor()

    def worker(num, t):
        if(t): t.join()
#        SELECT * FROM ratings, movies where movieid <= 200 and movieid1 <= 200 and movieid >= 100 and movieid1 >= 100 and movieid = movieid1 order by movieid ASC
        cmd = "INSERT INTO {0}  SELECT * FROM {1}, {2} WHERE {3} >= {5} and {4} >= {5} and {3} < {6} and {4} < {6} and {3}={4}".format(OutputTable, InputTable1, InputTable2, Table1JoinColumn, Table2JoinColumn, MIN_KEY + MAX_KEY*((num-1)/5.0), MIN_KEY + (MAX_KEY)*(num/5.0), )
        print cmd
        cursor.execute(cmd)


    threads = (THREAD_NUM+1)*[None]
    cursor.execute("SELECT MAX({0}) FROM {1}".format(Table1JoinColumn, InputTable1))
    MAX_KEY = cursor.fetchone()[0] + 1
    cursor.execute("SELECT MIN({0}) FROM {1}".format(Table1JoinColumn, InputTable1))
    MIN_KEY = cursor.fetchone()[0]

    cursor.execute("DROP TABLE IF EXISTS "+OutputTable)
    cmd = "CREATE TABLE {0} AS SELECT * FROM {1}, {2} WHERE 1=2".format(OutputTable, InputTable1, InputTable2)
    print cmd
    cursor.execute(cmd)
    for i in xrange(1, THREAD_NUM+1):
        t = threading.Thread(target=worker, args=(i, threads[i-1]))
        threads[i] = t
        t.start()

    threads[-1].join()

    cursor.close()
    openconnection.commit()

################### DO NOT CHANGE ANYTHING BELOW THIS #############################


# Donot change this function
def getOpenConnection(user='postgres', password='1234', dbname='ddsassignment3'):
    return psycopg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")

# Donot change this function
def createDB(dbname='ddsassignment3'):
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
        print 'A database named {0} already exists'.format(dbname)

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
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

# Donot change this function
def saveTable(ratingstablename, fileName, openconnection):
    try:
        cursor = openconnection.cursor()
        cursor.execute("Select * from %s" %(ratingstablename))
        data = cursor.fetchall()
        openFile = open(fileName, "w")
        for row in data:
            for d in row:
                openFile.write(`d`+",")
            openFile.write('\n')
        openFile.close()
    except psycopg2.DatabaseError, e:
        if openconnection:
            openconnection.rollback()
        print 'Error %s' % e
        sys.exit(1)
    except IOError, e:
        if openconnection:
            conn.rollback()
        print 'Error %s' % e
        sys.exit(1)
    finally:
        if cursor:
            cursor.close()

if __name__ == '__main__':
    try:
	# Creating Database ddsassignment3
	print "Creating Database named as ddsassignment3"
	createDB();

	# Getting connection to the database
	print "Getting connection from the ddsassignment3 database"
	con = getOpenConnection();

	# Calling ParallelSort
	print "Performing Parallel Sort"
	ParallelSort(FIRST_TABLE_NAME, SORT_COLUMN_NAME_FIRST_TABLE, 'parallelSortOutputTable', con);

	# Calling ParallelJoin
	print "Performing Parallel Join"
	ParallelJoin(FIRST_TABLE_NAME, SECOND_TABLE_NAME, JOIN_COLUMN_NAME_FIRST_TABLE, JOIN_COLUMN_NAME_SECOND_TABLE, 'parallelJoinOutputTable', con);

	# Saving parallelSortOutputTable and parallelJoinOutputTable on two files
	saveTable('parallelSortOutputTable', 'parallelSortOutputTable.txt', con);
	saveTable('parallelJoinOutputTable', 'parallelJoinOutputTable.txt', con);

	# Deleting parallelSortOutputTable and parallelJoinOutputTable
	deleteTables('parallelSortOutputTable', con);
       	deleteTables('parallelJoinOutputTable', con);

        if con:
            con.close()

    except Exception as detail:
        print "Something bad has happened!!! This is the error ==> ", detail
