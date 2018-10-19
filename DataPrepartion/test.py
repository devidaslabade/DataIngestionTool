# -*- coding: utf-8 -*-
"""
Created on Tue Oct 16 17:46:25 2018

@author: aj250046
"""

from pyspark.sql import SparkSession
import json
import pandas as pd

            
src = pd.read_json('..\source\src.json')
srcColMap = pd.read_json('..\source\srcCols.json')
dest = pd.read_json('..\dest\dest.json')
destColMap = pd.read_json('..\dest\destCols.json')
prc = pd.read_json('..\process\prc.json')
maps = pd.read_json('..\process\colMapping.json')

#print(srcColMap)

#pprint(prc)
for prcIdx, prcRow in prc.iterrows():
    query=""
    mapTab=maps[maps['mapId']==prcRow['mapId']]
    #print(mapTab)
    for mapId,mapRow in mapTab.iterrows() :
        srcColName=srcColMap[(srcColMap['srcId']== mapRow['srcId']) & (srcColMap['colId']== mapRow['srcColId'])]
        destColName=destColMap[(destColMap['destId']== mapRow['destId']) & (destColMap['colId']== mapRow['destColId'])]
        query=query+" \'"+srcColName['colName'].str.cat()+" as "+destColName['colName'].str.cat()+"\',"
    print(query[:-1])

            

