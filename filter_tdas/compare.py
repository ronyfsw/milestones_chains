import pandas as pd
cols = ['ID', 'ChainID', 'TaskID', 'TaskType']
milestones = pd.read_parquet('milestones.parquet', columns=cols)
milestones_tdas = pd.read_parquet('tdas_milestones.parquet', columns=cols)
print(len(milestones), len(milestones_tdas))
print(len(milestones.drop_duplicates()), len(milestones_tdas.drop_duplicates()))
milestones_tdas1 = milestones_tdas[milestones_tdas['TaskType'] != 'TT_Task']
print(len(milestones_tdas1))
