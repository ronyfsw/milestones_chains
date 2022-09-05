from modules.config import *
query = "SELECT * FROM {db}.{t} limit 100000".format(db=db_name, t=results_table)
print(query)
result = pd.read_sql(query, engine)
print(result.head())
print(result.info())
# result = result[:10000]
result.to_excel('result.xlsx', index=False)