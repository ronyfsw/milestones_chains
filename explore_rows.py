import pandas as pd
pd.set_option("display.max_rows", None, "display.max_columns", None, 'display.max_colwidth', 100)

df = pd.read_parquet('results_copy_0.parquet')
print(df.head())
print(df.info())
unique_chains = len(df['ChainID'].unique())
print('{n} unique chains'.format(n=unique_chains))
df.to_excel('EMS_DCMA_DD_23.08.xlsx', index=False)