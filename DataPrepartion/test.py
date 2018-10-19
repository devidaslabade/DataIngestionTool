import json
import pandas as pd
'''
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

for prcKey, prcVal in prc.items():
    print(prcVal['PrcName'])
    collst=colMap[prcVal['ColMapping']]
    for clm in collst :
        print(type(clm))
        print( clm['srcId'] , clm['destId'])
        #print(srcColMap[clm['srcId']])
        for key in srcColMap[clm['srcId']] :
            print (key)'''
            
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

            