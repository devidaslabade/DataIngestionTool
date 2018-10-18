# -*- coding: utf-8 -*-
"""
Created on Tue Oct 16 17:46:25 2018

@author: aj250046
"""

from pyspark.sql import SparkSession
import json
import pandas as pd

src = json.loads(open('..\source\src2.json', encoding='utf-8').read())
srcColMap = json.loads(open('..\source\srcCols.json', encoding='utf-8').read())
dest = json.loads(open('..\dest\dest.json', encoding='utf-8').read())
prc = json.loads(open('..\process\prc.json', encoding='utf-8').read())
colMap=json.loads(open('..\process\colMapping.json', encoding='utf-8').read())

dff=pd.DataFrame(src)
print(dff)
#help(pd.read_json)

tff=pd.read_json('..\source\src2.json')
print(tff)

#df = pd.DataFrame(list(data.items()))

'''for prcKey, prcVal in prc.items():
    print(prcVal['PrcName'])
    collst=colMap[prcVal['ColMapping']]
    for clm in collst :
        print(type(clm))
        print( clm['srcId'] , clm['destId'])
        #print(srcColMap[clm['srcId']])
        for key in srcColMap[clm['srcId']] :
            print (key)'''

