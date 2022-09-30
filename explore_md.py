import pandas as pd
nodes = ('MI20-EMS-N2-526200', 'MI20-EMS-N2-526210', 'MI20-EMS-N2-526220')
df = pd.read_excel('metadata_duration.xlsx')
for node in nodes:
    print(node, df['TaskType'][df['ID'] == node].values[0])
