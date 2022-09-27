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

def filter_tdas(id, chain, chain_nodes_types):
	milestone_chain = []
	for node in chain:
		if chain_nodes_types[node] != 'TT_Task':
			milestone_chain.append(node)
	milestone_chain = node_delimiter.join(milestone_chain)
	return (id, milestone_chain)


def growReproduce(map_or_step):
	cid, chain, next_steps = None, None, None
	process_id = map_or_step[0]
	chain_key = map_or_step[1]
	successors = map_or_step[2]
	growth_node = successors[0]
	pointers = successors[1:]

	scaffolds_dict = os.path.join(scaffolds_path, 'scaffolds_{p}.npy'.format(p=process_id))
	scaffolds = np.load(scaffolds_dict, allow_pickle=True)[()]
	previous_step_chain = scaffolds[chain_key]

	if previous_step_chain:
		# Growth
		previous_step_chain = previous_step_chain.split(node_delimiter)
		chain = previous_step_chain + [growth_node]
		chain = node_delimiter.join(chain)
		cid = encode_string(chain)

		# Reproduction
		next_steps = []
		for pointer in pointers:
			pointer = (pointer,)
			next_step = (process_id, chain_key, pointer)
			next_steps.append(next_step)
		next_steps = tuple(next_steps)

	return cid, chain, next_steps