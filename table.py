########################## option 1 ##########################
with connection.cursor(dictionary=True) as cursor:
  cursor.execute("...")
  results=cursor.fetchall()
connection.close()

# header list
header = list(results[0].keys())

# length 구하기
length = [len(h) for h in header]
for result in results:
  for i in range(len(header)):
    if length[i] < len(str(result[header[i]])):
      length[i] = len(str(result[header[i]]))

length = [l + 4 for l in length]

# table 출력
print("-" * sum(length))
header_str = ""
for i in range(len(header)):
  header_str += header[i].ljust(length[i])
print(header_str)
print("-" * sum(length))
for result in results:
  item_str = ""
  for i in range(len(header)):
    item_str += str(result[header[i]]).ljust(length[i])

  print(item_str)
print("-" * sum(length))

########################## option 2 ##########################
from tabulate import tabulate

print(tabulate(results, headers='keys', tablefmt='psql'))