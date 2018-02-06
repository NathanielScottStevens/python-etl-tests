import pandas as pd
import pandas.io.sql as psql
import numpy as np
import time
# pip install mysql-connector-python-rf
import mysql.connector 
import pyodbc

# Database from https://github.com/datacharmer/test_db
def main():

    employees = get_table('employees')
    salaries = get_table('salaries')

    #employees.set_index('emp_no')
    #salaries.set_index('emp_no')

    start = time.time()
    joined = pd.merge(employees, salaries, how='left', on='emp_no')
    print(time.time() - start)


def get_table(table):
    connection = pyodbc.connect('DRIVER={MySQL ODBC 5.3 Unicode Driver};Server=localhost;Database=employees;Uid=root;Pwd=root;')
    cursor = connection.cursor()
    query = f"SELECT * FROM {table}"
    return psql.read_sql(query, connection) 




if __name__ == "__main__":
    main()
    #start = time.time()
    #a = pd.DataFrame(np.random.randint(0, 10000, size=(1000, 2)), columns=['a', 'b'])
    #a['c'] = a['a'] + a['b']
    #a['c'] = a.apply(lambda row: row['a'] + row['b'], axis=1)
    #print(time.time() - start)
