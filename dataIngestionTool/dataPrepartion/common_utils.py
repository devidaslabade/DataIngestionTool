import json
from jsonschema import validate

def validateDataWithSchema(row, jsonSchemaMap):
    print("index:: ")
    updateDF = []
    for x in row:
        try:
            data = json.loads(x)
            #print(data.collect())
            validate(data, jsonSchemaMap)
            data["valid"] = "Y"
            data["errMsg"] = ""
            updateDF.append(data)
            print("OK")
        except Exception as e:
            print(str(e.message))
            data["valid"] = "N"
            data["errMsg"] = str(e.message)
            updateDF.append(data)
    print("Finally")
    print(updateDF)
    return updateDF