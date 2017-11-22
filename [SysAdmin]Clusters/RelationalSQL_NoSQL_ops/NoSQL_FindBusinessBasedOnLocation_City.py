#!/usr/bin/python2.7
#
# Assignment3 Interface
#   ID: 1211181735
# Name: Ying-Jheng Chen
#

from pymongo import MongoClient
import os
import sys
import json
import re
import math
import codecs

def FindBusinessBasedOnCity(cityToSearch, saveLocation1, collection):
    matched = collection.find({"city":re.compile(cityToSearch, re.IGNORECASE)})
    content = []
    for m in matched:
        content.append(m['name'] + '$' + m['full_address'] + '$' + m['city']+'$'+m['state'] + os.linesep)

    with codecs.open(saveLocation1, "w", "utf-8") as f:
        for line in content:
            f.write(line.upper())
    
def FindBusinessBasedOnLocation(categoriesToSearch, myLocation, maxDistance, saveLocation2, collection):
    content = []
    R=3959
    lat1 = float(myLocation[0])
    logt1 = float(myLocation[1])
    phi1 = math.radians(lat1)
    '''
    s_re = []
    for s in categoriesToSearch:
        s_re.append(re.compile(s, re.IGNORECASE))

    matched = collection.find({"categories": {"$in": s_re}})
    '''
    matched = collection.find({"categories": {"$in":categoriesToSearch}})
    for m in matched:
        lat2 = float(m["latitude"])
        logt2 = float(m["longitude"])
        phi2 = math.radians(lat2)

        dphi = math.radians(lat2 -lat1)
        dlambda = math.radians(logt2 - logt1)

        a = math.sin(dphi/2) * math.sin(dphi/2) + math.cos(phi1) * math.cos(phi2) * math.sin(dlambda/2) * math.sin(dlambda/2)

        c = 2 * math.atan2(math.sqrt(a) , math.sqrt(1-a));
        d = R * c
        if d < maxDistance:
            content.append(m["name"]+os.linesep)

    with codecs.open(saveLocation2, "w", "utf-8") as f:
        for line in content:
            f.write(line.upper())
