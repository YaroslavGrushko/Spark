import pandas as pd
from sqlalchemy import create_engine
# file = 'testfile.csv'

# print(pd.read_csv(file, nrows=5))

with open('testfile_short1.csv', 'r') as original: data = original.read()
for i in range(2):
    with open('testfile_short3.csv', 'a') as modified: modified.write(data)