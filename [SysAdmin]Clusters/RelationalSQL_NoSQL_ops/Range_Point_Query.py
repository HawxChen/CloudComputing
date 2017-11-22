#!/usr/bin/python2.7 #
# Assignment2 Interface
#ASU ID: 1211181735
#Name: Ying-Jheng Chen
import psycopg2
import os
import sys
# Donot close the connection inside this file i.e. do not perform openconnection.close()

rangePrefix = "rangeratingspart"
prangePrefix= "RangeRatingsPart"
roundPrefix = "roundrobinratingspart"
proundPrefix = "RoundRobinRatingsPart"
def get_range_result(cur, prefix, pprefix, result,ratingMinValue, ratingMaxValue):
    cur.execute("SELECT count(table_name) FROM information_schema.tables WHERE table_schema='public' and table_name SIMILAR TO '{0}\\d+'".format(prefix))
    num_tables = (cur.fetchone()[0])
    for idx in xrange(num_tables):
        cur.execute("SELECT * FROM {0}".format(prefix + str(idx)))
        items = cur.fetchall()
        for uid, mid, rating in items:
            if(rating >= ratingMinValue and rating <= ratingMaxValue):
#                print uid, mid, rating
                result.append("{0}{1},{2},{3},{4}\n".format(pprefix,idx,uid,mid,rating))


def RangeQuery(ratingsTableName, ratingMinValue, ratingMaxValue, openconnection):
    filename = "RangeQueryOut.txt"
    #Implement RangeQuery Here.
    #ratingMinValue <= Key <= RatingMaxValue
    result =[]
    with openconnection.cursor() as cur:
        get_range_result(cur, rangePrefix, prangePrefix, result,ratingMinValue, ratingMaxValue )
        get_range_result(cur, roundPrefix, proundPrefix, result,ratingMinValue, ratingMaxValue )
#        print "".join(result)
     
    with open(filename,"w+") as f:
        f.truncate()
        f.write("".join(result))

    return

def get_point_result(cur, prefix, pprefix, result, ratingValue):
    cur.execute("SELECT count(table_name) FROM information_schema.tables WHERE table_schema='public' and table_name SIMILAR TO '{0}\d+'".format(prefix))
    num_tables = (cur.fetchone()[0])
    for idx in xrange(num_tables):
        cur.execute("SELECT * FROM {0}".format(prefix + str(idx)))
        items = cur.fetchall()
        for uid, mid, rating in items:
            if(rating == ratingValue):
#                print uid, mid, rating
                result.append("{0}{1},{2},{3},{4}\n".format(pprefix,idx,uid,mid,rating))

def PointQuery(ratingsTableName, ratingValue, openconnection):
    filename = "PointQueryOut.txt"
    #Implement PointQuery Here.
    result =[]
    with openconnection.cursor() as cur:
        get_point_result(cur, rangePrefix, prangePrefix, result,ratingValue)
        get_point_result(cur, roundPrefix, proundPrefix, result,ratingValue)
#        print "".join(result)

    with open(filename,"w+") as f:
        f.truncate()
        f.write("".join(result))
    return
