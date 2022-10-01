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

def drop_chain_overlaps(chains):
    chains.sort(key=len)
    small = []
    for index1, chain1 in enumerate(chains):
        if ((chain1 not in small) & (index1 < len(chains)-2)):
            for index2, chain2 in enumerate(chains):
                if ((chain1 != chain2) & (chain1 in chain2)):
                    small.append(chain1)
                    chains = list(set(chains).difference(set(small)))
                    break
    small = list(set(small))
    keep = [c for c in chains if c not in small]
    return keep