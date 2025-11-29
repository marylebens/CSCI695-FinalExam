import happybase

connection = happybase.Connection('localhost', port=9090)
table = connection.table('college_salaries')

print("First 5 records:")
count = 0
for key, data in table.scan(limit=5):
    print(f"\nRow key: {key.decode('utf-8')}")
    for column, value in data.items():
        print(f"  {column.decode('utf-8')}: {value.decode('utf-8')}")
    count += 1

print(f"\n\nTotal records in table:")
row_count = sum(1 for _ in table.scan())
print(row_count)

connection.close()