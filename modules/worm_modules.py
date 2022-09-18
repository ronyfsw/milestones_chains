from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from libraries import *
from graphs import *
from config import *
from encoders import *

def get_terminal_nodes(G):
	Gnodes = list(G.nodes())
	isolates = graph_isolates(G)
	return [n for n in Gnodes if ((G.out_degree(n) == 0) & (n not in isolates))]

def get_recent_id(map_or_steps_dict):
	'''
	Return the ID for the last decendant born
	:param map_or_steps_dict(dict): Worms' growth and birth map_or_steps
	'''
	map_or_steps = []
	for k, v in map_or_steps_dict.items(): map_or_steps += v
	if map_or_steps:
		return max([d[0] for d in map_or_steps])
	else:
		return 1

def validate(chain, source_pairs):
	'''
	Validate a single chain by the successor-predecessor relationship of its nodes
	:param chain(list): A chain to validate
	:param source_pairs(dict): The successor-predecessor nodes relationship in the source graph data
	:return: Successor-predcessor pairs identified in the chain that are not part of the source pairs
	'''

	if chain:
		pairs = []
		chain_pairs = []
		for index, id in enumerate(chain):
			if index < len(chain)-1:
				chain_pairs.append((id, chain[index+1]))
		pairs += chain_pairs
		pairs = list(set(pairs))
		pairs_in_source = [p for p in pairs if p in source_pairs]
		error_pairs = list(set(pairs).difference(set(pairs_in_source)))
	else:
		error_pairs = []
	return error_pairs

def growReproduce(map_or_step):
	cid, chain, next_steps = None, None, None
	process_id = map_or_step[0]
	chain_key = map_or_step[1]
	#print('**** chain key:', chain_key)
	successors = map_or_step[2]
	growth_node = successors[0]
	pointers = successors[1:]

	#previous_step_chain = redisClient.hget('scaffolds', chain_key)
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