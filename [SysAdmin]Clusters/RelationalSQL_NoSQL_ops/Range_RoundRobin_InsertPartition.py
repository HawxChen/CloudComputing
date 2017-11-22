#!/usr/local/bin/python2.7

"""
#for LINUX
#!/usr/bin/python2.7
"""

import psycopg2 as pg2
import decimal
import cStringIO as csio
import traceback

MY_INPUT_FILE_PATH = './test_data.dat'
#MY_INPUT_FILE_PATH = './ratings.dat'
DATABASE_NAME = 'postgres' # TODO: Change these as per your code MY_RATINGS_TABLE = 'ratings'
MY_RATINGS_TABLE = 'ratings'
MY_RANGE_TABLE_PREFIX = 'range_part'
MY_RROBIN_TABLE_PREFIX = 'rrobin_part'
MY_USER_ID_COLNAME = 'userid'
MY_MOVIE_ID_COLNAME = 'movieid'
MY_RATING_COLNAME = 'rating'
MY_ACTUAL_ROWS_IN_INPUT_FILE = 20  # Number of lines in the input file
BUF_SIZE= 33128
HIGHEST_POINT = 5
NUM_PARTITIONS = 5
UID_IDX = 0
MID_IDX = 1
RAT_IDX = 2
NAME_IDX = 0
CSIO_IDX = 1
LIST_IDX = 2
MY_META_TABLE_NAME = 'metatable'
MY_META_TABLE_NAME_COLNAME = 'table_name'
MY_META_PARTITION_COLNAME = 'num_partition'
MY_META_NEXT_PARTITION_COLNAME = 'next_partition'


def getopenconnection(user='postgres', password='1234', dbname='dds_assgn1'):
    return pg2.connect("dbname='" + dbname + "' user='" + user + "' host='localhost' password='" + password + "'")


def createOnetable (table_name, openconnection):
    with openconnection.cursor() as cur:
        cur.execute('CREATE TABLE IF NOT EXISTS {0} ({1} INTEGER, {2} INTEGER, {3} float(1));'.format(table_name, MY_USER_ID_COLNAME, MY_MOVIE_ID_COLNAME, MY_RATING_COLNAME))
#        cur.execute('CREATE TABLE IF NOT EXISTS {0} ({1} CHAR(10), {2} CHAR(10), {3} CHAR(5));'.format(table_name, MY_USER_ID_COLNAME, MY_MOVIE_ID_COLNAME, MY_RATING_COLNAME))
#        cur.execute('CREATE TABLE IF NOT EXISTS {0} ({1} VARCHAR(10), {2} VARCHAR(10), {3} VARCHAR(5));'.format(table_name, MY_USER_ID_COLNAME, MY_MOVIE_ID_COLNAME, MY_RATING_COLNAME))


def loadratings(ratingstablename, ratingsfilepath, openconnection):
    with open(ratingsfilepath) as infile:
        vfile =  csio.StringIO()
        lines = infile.readlines()
        for idx in xrange(len(lines)):
#            lines[idx] = lines[idx].rsplit('::',1)[0].replace("::",":")  + '\n'
            vfile.write(lines[idx].rsplit('::',1)[0].replace("::",":")  + '\n')
        vfile.seek(0)

        with openconnection.cursor() as cur:
            createOnetable(ratingstablename, openconnection)
            cur.copy_from(vfile, ratingstablename, ':', size=BUF_SIZE)
            cur.execute("INSERT INTO {0} VALUES ('{1}', {2}, {3})".format(MY_META_TABLE_NAME, ratingstablename ,0, 0), openconnection)

    pass



def drange(start, end, step):
    while start < end:
        yield float(start)
        start += decimal.Decimal(step)

def exec_sql_cmd(cmd, con):
    with con.cursor() as cur:
        cur.execute(cmd)

def createIOset(prefix, numberofpartitions, openconnection):
    range_table_names = []
    for idx in xrange(numberofpartitions):
        range_table_names.append((prefix + str(idx), csio.StringIO(), []))
        createOnetable(range_table_names[idx][NAME_IDX], openconnection)
        exec_sql_cmd("INSERT INTO {0} VALUES ('{1}', {2}, {3})".format(MY_META_TABLE_NAME,range_table_names[-1][0] ,numberofpartitions, 0), openconnection)
    return range_table_names


def range_getIDX(rate, interval, numberofpartitions):
    if rate == HIGHEST_POINT:
        IDX = numberofpartitions - 1
    else:
        IDX = int(rate/interval)
        if IDX != 0 and rate == IDX*interval: IDX -= 1

    return IDX

def rangepartition(ratingstablename, numberofpartitions, openconnection):
    #Create All tables needed
    range_table_names = createIOset(MY_RANGE_TABLE_PREFIX, numberofpartitions, openconnection)
    MAX_INDEX = numberofpartitions - 1

    rows = None
    with openconnection.cursor() as cur:
        cur.execute("SELECT * FROM {0}".format(ratingstablename))
        rows = cur.fetchall()

        interval = float(HIGHEST_POINT)/numberofpartitions
        for item in rows:
            IDX = range_getIDX(item[RAT_IDX], interval, numberofpartitions)
#            range_table_names[IDX][LIST_IDX].append(":".join(item)+'\n')
#            print IDX, (range_table_names[IDX][LIST_IDX])[-1],
            range_table_names[IDX][CSIO_IDX].write("{0}:{1}:{2}\n".format(item[0], item[1], item[2]))

        for idx in xrange(numberofpartitions):
            range_table_names[idx][CSIO_IDX].seek(0)
            cur.copy_from(range_table_names[idx][CSIO_IDX], range_table_names[idx][NAME_IDX], ':', size=BUF_SIZE)
    pass

def roundrobinpartition(ratingstablename, numberofpartitions, openconnection):
    range_table_names = createIOset(MY_RROBIN_TABLE_PREFIX, numberofpartitions, openconnection)

    rows = None
    with openconnection.cursor() as cur:
        cur.execute("SELECT * FROM {0}".format(ratingstablename))
        rows = cur.fetchall()
        length = len(rows)
        idx = 0
        while idx < length:
            #range_table_names[idx%numberofpartitions][LIST_IDX].append("{0}\n".format(":".join(rows[idx])))
            #print idx%numberofpartitions, (range_table_names[idx%numberofpartitions][LIST_IDX])[-1],
            range_table_names[idx%numberofpartitions][CSIO_IDX].write("{0}:{1}:{2}\n".format(rows[idx][0], rows[idx][1], rows[idx][2]))
            #range_table_names[idx%numberofpartitions][CSIO_IDX].write(":".join(rows[idx]+'\n'))
            idx += 1

        cur.execute("UPDATE {0} SET {1} = {2} WHERE {3} = '{4}'".format(MY_META_TABLE_NAME, MY_META_NEXT_PARTITION_COLNAME, idx % numberofpartitions, MY_META_TABLE_NAME_COLNAME, MY_RROBIN_TABLE_PREFIX+'0'))

        for idx in xrange(numberofpartitions):
            range_table_names[idx][CSIO_IDX].seek(0)
            cur.copy_from(range_table_names[idx][CSIO_IDX], range_table_names[idx][NAME_IDX], ':', size=BUF_SIZE)

    pass

def rangeinsert(ratingstablename, userid, itemid, rating, openconnection):
    """
    with openconnection.cursor() as cur:
        cur.execute(
        numofpartitions = cur.fetchone()
    """
    with openconnection.cursor() as cur:
        cur.execute("SELECT {0} FROM {1} WHERE {2} = '{3}'".format(MY_META_PARTITION_COLNAME, MY_META_TABLE_NAME, MY_META_TABLE_NAME_COLNAME, MY_RANGE_TABLE_PREFIX+'0'))
        numberofpartitions = int(cur.fetchone()[0])

        interval = float(HIGHEST_POINT)/numberofpartitions
        cur.execute("INSERT INTO {0} VALUES ({1}, {2}, {3})".format(MY_RANGE_TABLE_PREFIX+str(range_getIDX(rating, interval, numberofpartitions)), userid, itemid, rating))

    pass


def roundrobininsert(ratingstablename, userid, itemid, rating, openconnection):

    with openconnection.cursor() as cur:
        cur.execute("SELECT * FROM {0} WHERE {1} = '{2}'".format(MY_META_TABLE_NAME, MY_META_TABLE_NAME_COLNAME, MY_RROBIN_TABLE_PREFIX+'0'))
        ret_cur = cur.fetchone()
        numberofpartitions = ret_cur[1]
        next_partition = ret_cur[2]
        cur.execute("INSERT INTO {0} VALUES ({1}, {2}, {3})".format(MY_RROBIN_TABLE_PREFIX+str(next_partition), userid, itemid, rating))


        cur.execute("UPDATE {0} SET {1} = {2} WHERE {3} = '{4}'".format(MY_META_TABLE_NAME, MY_META_NEXT_PARTITION_COLNAME, (next_partition + 1) %  numberofpartitions, MY_META_TABLE_NAME_COLNAME, MY_RROBIN_TABLE_PREFIX+'0'))
    pass



def deletepartitionsandexit(openconnection):
    with openconnection.cursor() as cur:
        cur.execute("SELECT {0} FROM {1}".format(MY_META_TABLE_NAME_COLNAME, MY_META_TABLE_NAME))
        ret_table_names = cur.fetchall()
        for item in ret_table_names:
            cur.execute("DROP TABLE IF EXISTS " + item[0] )
    pass

def create_db(dbname):
    """
    We create a DB by connecting to the default user and database of Postgres
    The function first checks if an existing database exists for a given name, else creates it.
    :return:None
    """
    # Connect to the default database
    con = getopenconnection(dbname)
    con.set_isolation_level(pg2.extensions.ISOLATION_LEVEL_AUTOCOMMIT)
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
    con.close()


# Middleware
def before_db_creation_middleware():
    # Use it if you want to
    pass


def after_db_creation_middleware(databasename):
    # Use it if you want to

    pass


def before_test_script_starts_middleware(openconnection, databasename):
    # Use it if you want to
    with openconnection.cursor() as cur:
        cur.execute("DROP SCHEMA public CASCADE; CREATE SCHEMA public;")
        cur.execute('CREATE TABLE IF NOT EXISTS {0} ({1} VARCHAR(20), {2} INTEGER, {3} INTEGER);'.format(MY_META_TABLE_NAME, MY_META_TABLE_NAME_COLNAME, MY_META_PARTITION_COLNAME, MY_META_NEXT_PARTITION_COLNAME))
        cur.execute("INSERT INTO {0} VALUES ('{1}', {2}, {3})".format(MY_META_TABLE_NAME, MY_META_TABLE_NAME, 0, 0, openconnection))
    pass


def after_test_script_ends_middleware(openconnection, databasename):
    # Use it if you want to
    pass


if __name__ == '__main__':
    try:

        # Use this function to do any set up before creating the DB, if any
        before_db_creation_middleware()

        create_db(DATABASE_NAME)

        # Use this function to do any set up after creating the DB, if any
        after_db_creation_middleware(DATABASE_NAME)

        with getopenconnection(dbname=DATABASE_NAME) as con:
            # Use this function to do any set up before I starting calling your functions to test, if you want to
            before_test_script_starts_middleware(con, DATABASE_NAME)

            # Here is where I will start calling your functions to test them. For example,
            loadratings(MY_RATINGS_TABLE, MY_INPUT_FILE_PATH, con)
            # ###################################################################################
            # Anything in this area will not be executed as I will call your functions directly
            # so please add whatever code you want to add in main, in the middleware functions provided "only"
            # ###################################################################################
            rangepartition(MY_RATINGS_TABLE, NUM_PARTITIONS, con)
            rangeinsert(None, 100,2,3, con)
            roundrobinpartition(MY_RATINGS_TABLE, NUM_PARTITIONS, con)
            roundrobininsert(None, 100, 2, 3, con)

            # Use this function to do any set up after I finish testing, if you want to
            after_test_script_ends_middleware(con, DATABASE_NAME)
            con.commit()
            deletepartitionsandexit(con)

    except Exception as detail:
        print "OOPS! This is the error ==> ", detail,
        traceback.print_exc()

        with getopenconnection(dbname=DATABASE_NAME) as con:
            deletepartitionsandexit(con)
