import xml.etree.ElementTree as ET
import pandavro as pdx
import pandas as pd
import re
import copy
#import xml
import json
from avro.datafile import DataFileReader
from avro.io import DatumReader

tree = ET.parse("D:\\xml_adat\\filmek.xml")
root = tree.getroot()

#1. feladat; forrás hibás filmcímet tartalmaz -> Back 2 the Future -> ezt replace-ve: Back to the Future legyen!
#for movie in root.iter('movie'):
#    print(movie.attrib)

#1x van csak, Xpath
backToTheFuture = root.find("./genre/decade/movie[@title='Back 2 the Future']")
backToTheFuture.attrib["title"] = "Back to the Future"
#print(backToTheFuture.attrib)

#2. feladat; a 'multiple' érték van ,ahol 'False', van ahol 'No' ->{'multiple': 'False'} Blu-ray
#for multi in root.findall("./genre/decade/movie/format"):
#    print(multi.attrib, multi.text)

# Legyen csere: False -> No -ra!
for multi in root.findall("./genre/decade/movie/format"):
    match = re.search(',',multi.text)
    if match:
        multi.set('multiple','Yes')
    else:
        multi.set('multiple','No')

#for mult in root.findall("./genre/decade/movie/format"):
#    print(mult.attrib, mult.text)

#3. feladat; Remove-oljuk az 'R' osztályzatot kapott filmek leírás értékét!
for movie in root.findall('description'):
    rate = movie.find('rating').text
    if rate == 'R':
        root.remove(movie)

#for rate in root.findall("./genre/decade/movie/description"):
#    print(rate.attrib, rate.text)


#4. feladat; a kijavított, módosított filet írjuk ki .xml file-ba!
#dictData = tree.write("D:\\xml_adat\\filmek_modified.xml")

#5. feladat; json beolvasása ->xml_to_json.py eredménye és annak .avro-ba írása

json_movies = open("D:\\xml_adat\\filmek_modified.json")
json_data = json.load(json_movies)
json_movies_df = pd.DataFrame.from_records(json_data)
#print(json_movies_df)

outPath = "D:\\xml_adat\\filmek_out.avro"
pdx.to_avro(outPath, json_movies_df)

movies_df_redux = pdx.from_avro(outPath)
print(type(users_df_redux))

with open(outPath, 'rb') as f:
    reader = DataFileReader(f, DatumReader())
    metadata = copy.deepcopy(reader.meta)
    schema_from_file = json.loads(metadata['avro.schema'])
    reader.close()
print(schema_from_file)


#outPath = "D:\\xml_adat\\filmek_out.avro"
#data_df = pd.DataFrame.from_records(dictData)
#print(data_df)
#pdx.to_avro(outPath, data_df)

#saved = pdx.read_avro(outPath)
#print(saved)
