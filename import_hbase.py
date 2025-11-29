import happybase
import pandas as pd
import time

time.sleep(5)

connection = happybase.Connection('localhost', port=9090)

print("Connected to HBase")

try:
    connection.delete_table('college_salaries', disable=True)
except:
    pass

connection.create_table(
    'college_salaries',
    {
        'school_info': dict(),
        'salary_info': dict()
    }
)

print("Created table")

df = pd.read_csv('salaries-by-college-type.csv')

table = connection.table('college_salaries')

for index, row in df.iterrows():
    row_key = str(index).zfill(5)
    
    table.put(row_key, {
        b'school_info:name': str(row['School Name']).encode('utf-8'),
        b'school_info:type': str(row['School Type']).encode('utf-8'),
        b'salary_info:starting_median': str(row['Starting Median Salary']).encode('utf-8'),
        b'salary_info:mid_career_median': str(row['Mid-Career Median Salary']).encode('utf-8'),
        b'salary_info:mid_career_10th': str(row['Mid-Career 10th Percentile Salary']).encode('utf-8'),
        b'salary_info:mid_career_90th': str(row['Mid-Career 90th Percentile Salary']).encode('utf-8')
    })
    
    if index % 10 == 0:
        print(f"Inserted {index} records")

print(f"Finished inserting {len(df)} records")

connection.close()