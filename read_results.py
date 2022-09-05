from modules.config import *
import mysql.connector as connection
query = "SELECT * FROM {db}.{t}".format(db=db_name, t=results_table)
print(query)
#mydb = connection.connect(host="localhost", database = 'Student',user="root", passwd="root",use_pure=True)
result = pd.read_sql(query, engine)
print(result.head())
print(result.info())
result = result[:10000]
result.to_excel('result.xlsx', index=False)