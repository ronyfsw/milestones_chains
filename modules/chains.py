from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from config import *
from tasks import *

def growReproduce_dicts(map_or_step):
	'''
	Extend chains and build the maps for the chains to be built in the next iteration
	:param map_or_step: The chain's next step instructions including the build_chains process
	building the chains, the chain key to identify the chain in the scaffolds, and the successors to
	use in extending the chains, or in building the guides for the next steps

	'''
	cid, chain, next_steps = None, None, None
	process_id = map_or_step[0]
	chain_key = map_or_step[1]
	successors = map_or_step[2]
	growth_tip = successors[0]
	initiators = successors[1:]

	scaffolds_dict = os.path.join(scaffolds_path, 'scaffolds_{p}.npy'.format(p=process_id))
	scaffolds = np.load(scaffolds_dict, allow_pickle=True)[()]
	if chain_key in list(scaffolds.keys()):
		previous_step_chain = scaffolds[chain_key]
		
		# Growth
		# An extention of a chain produced in previous step using of the growth tip successors
		previous_step_chain = previous_step_chain.split(node_delimiter)
		chain = previous_step_chain + [growth_tip]
		chain = node_delimiter.join(chain)
		cid = encode_string(chain)

		# Reproduction
		# Chain initiations from the growth-tip successors that were not exploited for growth
		next_steps = []
		for initiator in initiators:
			initiator = (initiator,)
			next_step = (process_id, chain_key, initiator)
			next_steps.append(next_step)
		next_steps = tuple(next_steps)

	return cid, chain, next_steps

# steps_chunk = [(39265, 1, ('196.4',))]
# next_journeys_steps = [(pid, 1, root_successors)]
def growReproduce_redis(map_or_step):
	cid, chain, next_steps = None, None, None
	chain_key = map_or_step[1]
	successors = map_or_step[2]
	growth_tip = successors[0]
	initiators = successors[1:]

	previous_step_chain = redisClient.hget('scaffolds', chain_key)

	if previous_step_chain:
		# Growth
		previous_step_chain = previous_step_chain.split(node_delimiter)
		chain = previous_step_chain + [growth_tip]
		chain = node_delimiter.join(chain)
		cid = encode_string(chain)

		# Reproduction
		next_steps = []
		for pointer in initiators:
			pointer = (pointer,)
			next_step = (chain_key, pointer)
			next_steps.append(next_step)
		next_steps = tuple(next_steps)

	return cid, chain, next_steps

def drop_chain_overlaps(chains):
	# Filter a list of chains of short chains that are included in longer chains
	# If chains = [A-B, A-B-C] only A-B-C will be included in the filtered version (keep)
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
	'''
	Count the chains in each of the scaffold dictionaries built
	'''
	scaffold_chains_count = 0
	scaffolds_files = os.listdir(scaffolds_path)
	for scaffolds_file in scaffolds_files:
		scaffold_path = os.path.join(scaffolds_path, scaffolds_file)
		scaffold = np.load(scaffold_path, allow_pickle=True)[()]
		scaffold_chains_count += len(scaffold)
	return scaffold_chains_count

