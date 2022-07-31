import os
import sys
import matplotlib.pyplot as plt
import pandas as pd
import sys
mp = '/home/rony/Projects_Code/Cluster_Activities/modules'
if mp not in sys.path: sys.path.append(mp)
from plots import *
from modules.config import *

tracker = pd.read_excel(os.path.join(experiment_path, 'tracker.xlsx'))
no_chain_steps = tracker[tracker['chain_built'] == 0]
built_chain_steps = tracker[(tracker['chain_built'] == 1) & (tracker['new_chain'] == 0)]
new_chain_steps = tracker[tracker['new_chain'] == 1]

group_data = {'steps': tracker, 'noChain': no_chain_steps,\
            'builtChain': built_chain_steps, 'newChain': new_chain_steps}

#print(tracker.columns)
#val_cols = [c for c in df.columns if c != 'step']

def plot_df(df, name, xy_pairs):
	for x_col, y_col in xy_pairs:
		x, y = list(df[x_col]), list(df[y_col])
		plt.scatter(x, y, marker='.', s=1)
		plt.xlabel(x_col)
		plt.ylabel(y_col)
		figname = '{n}_{a}_vs_{b}.png'.format(n=name, a=y_col, b=x_col)
		plt.savefig(os.path.join(plots_path, figname))
		plt.close()

xy_pairs = [('writed', 'stepd'), ('chains', 'assertd'), ('chains', 'writed'), ('chains', 'growthd'),
            ('chains', 'reproduced'), \
            ('chains', 'updated'), ('updated', 'stepd')]
ratio_pairs = [('chains', col) for col in tracker.columns if 'ratio' in col]
xy_pairs += ratio_pairs
print(xy_pairs)
for group, df in group_data.items(): plot_df(df, group, xy_pairs)

# for col in val_cols:
# 	print(col)
# 	x = list(tracker[col])
# 	x = [i for i in x if i]
# 	histogram_stats(x, col, col, os.path.join(plots_path, '{c}.png'.format(c=col)))
