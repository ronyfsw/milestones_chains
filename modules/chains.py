from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from config import *
from encoders import *

def growReproduce(map_or_step):
	cid, chain, next_steps = None, None, None
	process_id = map_or_step[0]
	chain_key = map_or_step[1]
	successors = map_or_step[2]
	growth_node = successors[0]
	pointers = successors[1:]

	scaffolds_dict = os.path.join(scaffolds_path, 'scaffolds_{p}.npy'.format(p=process_id))
	scaffolds = np.load(scaffolds_dict, allow_pickle=True)[()]
	if chain_key in list(scaffolds.keys()):
		previous_step_chain = scaffolds[chain_key]
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

def drop_chain_overlaps(chains):
    chains = list(set([c for c in chains if c]))
    chains.sort(key=len)
    exclude, keep = [], []
    while chains:
        chain1 = chains[0]
        chains.remove(chain1)
        for chain2 in chains:
            if chain1 in chain2:
                exclude.append(chain1)
                break
        if chain1 not in exclude: keep.append(chain1)
    return keep

def count_scaffolds_chains(scaffolds_path):
	scaffold_chains_count = 0
	scaffolds_files = os.listdir(scaffolds_path)
	for scaffolds_file in scaffolds_files:
		scaffold_path = os.path.join(scaffolds_path, scaffolds_file)
		scaffold = np.load(scaffold_path, allow_pickle=True)[()]
		scaffold_chains_count += len(scaffold)
	return scaffold_chains_count

