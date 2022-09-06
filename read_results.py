from modules.config import *
results_table = 'results'
query = "SELECT * FROM {db}.{t}".format(db=db_name, t=results_table)
print(query)
result = pd.read_sql(query, engine)
print(result.head())
print(result.info())
result_sample = result[:10000]
result.to_excel('result_sample.xlsx', index=False)
result.to_pickle('result_sample.pkl', index=False)