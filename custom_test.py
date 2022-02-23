from lstore.db import Database
from lstore.query import Query
from copy import deepcopy
from random import choice, randint, sample, seed

# Student Id and 4 grades
db = Database()
grades_table = db.create_table('Grades', 5, 0)
query = Query(grades_table)

records = {}

seed(3562901)

for i in range(0, 2):
    key = 1
    while key in records:
        key += 1
    records[key] = [key, randint(0, 20), randint(0, 20), randint(0, 20), randint(0, 20)]
    query.insert(*records[key])
    print('inserted', records[key])

for key in records:
    record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
    error = False
    for i, column in enumerate(record.user_data):
        if column != records[key][i]:
            error = True
    if error:
        print('select error on', key, ':', record, ', correct:', records[key])
    else:
        print('select on', key, ':', record)

for key in records:
    updated_columns = [None, None, None, None, None]
    for i in range(1, grades_table.num_columns):
        value = randint(0, 20)
        updated_columns[i] = value
        original = records[key].copy()
        records[key][i] = value
        did_update = query.update(key, *updated_columns)
        record = query.select(key, 0, [1, 1, 1, 1, 1])[0]
        error = False
        for j, column in enumerate(record.user_data):
            if column != records[key][j]:
                error = True
        if error:
            print('update error on', original, 'and', updated_columns, ':', record.user_data, ', correct:', records[key])
        else:
            print('update on', original, 'and', updated_columns, ':', record)
        updated_columns[i] = None

# keys = sorted(list(records.keys()))
# for c in range(0, grades_table.num_columns):
#     for i in range(0, 20):
#         r = sorted(sample(range(0, len(keys)), 2))
#         column_sum = sum(map(lambda key: records[key][c], keys[r[0]: r[1] + 1]))
#         result = query.sum(keys[r[0]], keys[r[1]], c)
#         if column_sum != result:
#             print('sum error on [', keys[r[0]], ',', keys[r[1]], ']: ', result, ', correct: ', column_sum)
#         else:
#             print('sum on [', keys[r[0]], ',', keys[r[1]], ']: ', column_sum)
