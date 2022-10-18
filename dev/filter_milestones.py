import pandas as pd
import numpy as np
results_df = pd.read_pickle('data_chains_duration.pkl')
print(results_df.info())
data_chains_duration = results_df[results_df['TaskType'] != 'TT_Task']
del results_df
print(data_chains_duration.info())
data_chains_duration = data_chains_duration.drop_duplicates()
print(data_chains_duration.info())
chain_ids = list(data_chains_duration['ChainID'].unique())
mscols = list(data_chains_duration.columns)
cols = mscols + ['MilestoneID']
chains_df = pd.DataFrame(columns=cols)
for chain_id in chain_ids:
    chain_df = data_chains_duration[data_chains_duration['ChainID'] == chain_id]
    milestones_ids = ['M{i}'.format(i=str(i)) for i in list(np.arange(1, len(chain_df)+1))]
    chain_df['MilestoneID'] = milestones_ids
    chains_df = pd.concat([chains_df, chain_df])

cols = mscols[:2] + ['MilestoneID'] + mscols[2:]
chains_df = chains_df[cols]
chains_df.to_pickle('chains_df.pkl')


