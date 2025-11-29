import pymongo
import pandas as pd

client = pymongo.MongoClient('localhost', 27017)
db = client['college_salaries']

df = pd.read_csv('salaries-by-college-type.csv')

records = df.to_dict('records')

collection = db['salaries_by_type']
collection.insert_many(records)

print(f"Inserted {len(records)} records into salaries_by_type collection")

client.close()