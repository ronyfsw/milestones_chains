from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from config import *
def scaffold_to_chain(scaffold):
    ids_scaffolds = redisClient.hgetall('scaffolds_nodes')
    chain = scaffold.split(node_delimiter)
    while re.findall('\D{2,}', chain[0]):
        cid = chain[0]
        chain = ids_scaffolds[cid].split(node_delimiter) + [chain[-1]]
        a = 0
    chain = node_delimiter.join(chain)
    return chain

def chain_to_rows(index_chain, links_types):
	rows = []
	chain_index, tasks = index_chain
	for index, task in enumerate(tasks):
		if index <= (len(tasks) - 2):
			next_task = tasks[index + 1]
		else:
			next_task = None
		if next_task:
			try:
				pair_edge_type = links_types[(task, next_task)]
			except KeyError:
				pair_edge_type = None
		else:
			pair_edge_type = None
		rows.append((task, chain_index, next_task, pair_edge_type))
	return rows

def filter_tdas(chain_node_types):
	id, chain, chain_nodes_types = chain_node_types
	milestone_chain = []
	for node in chain:
		if chain_nodes_types[node] != 'TT_Task':
			milestone_chain.append(node)
	milestone_chain = node_delimiter.join(milestone_chain)
	# if len(chain) >= 5:
	# 	with open('ms_chains_check.txt', 'a') as f:
	# 		f.write('chain: {c} | chain_dict:{d} | milestones:{m}'.format(c=str(chain), d=str(chain_nodes_types), m=str(milestone_chain)))
	return (id, milestone_chain)
