import pandas as pd

from modules.libraries import *
from modules.parsers import *
from modules.evaluate import *
from modules.graphs import *
from modules.worm_modules import *
from modules.config import *
from modules.encoders import *
def chains_from_redis(redis_db):
	redis_db = redisClient.hgetall(redis_db)
	chains = []
	db_keys = redis_db.keys()
	for db_key in db_keys:
		db_chain_str = redis_db[db_key]
		chains.append(db_chain_str)
	return chains

# Data
G = build_graph(file_path)
Gnodes = list(G.nodes())
source = pd.read_excel(source_path)
source_pairs = list(zip(source['Predecessor'], source['Successor']))

# Encode nodes
nodes_encoder = np.load('nodes_encoder.npy', allow_pickle=True)[()]
encoded = []
not_in = []
for p1, p2 in source_pairs:
	if all(y in Gnodes for y in [p1, p2]):
		encoded_pair = (nodes_encoder[p1], nodes_encoder[p2])
		encoded.append(encoded_pair)
	else:
		not_in.append((p1, p2))
source_pairs = encoded
source_pairs_count = len(set(source_pairs))
print('The program graph is built of {ns} unique successor-predecessor pairs'.format(ns=source_pairs_count))
G = nx.relabel_nodes(G, nodes_encoder)

Gnodes, Gedges = list(G.nodes()), G.edges()
terminal_nodes = get_terminal_nodes(G)

# Chains scaffold
#scaffolds = redisClient.hgetall('scaffolds')
scaffolds = chains_from_redis('scaffolds')
n1, n2 = len(scaffolds), len(set(scaffolds))
print('{n1} chains built ({n2} are unique)'.format(n1=n1, n2=n2))
chains_printout_path = os.path.join(experiment_path, 'scaffolds.txt')
chains_str = '\n'.join(scaffolds)
with open(chains_printout_path, 'w') as f: f.write(chains_str)

scaffolds = list(set(scaffolds))

# Chains built
chains_df = pd.read_sql('SELECT * FROM MCdb.{ct}'.format(ct=chains_table), con=conn)
print(chains_df.head())
built_chains_col = list(chains_df['chain'])
built_chains = list(set(built_chains_col))
n1, n2 = len(built_chains_col), len(built_chains)
print('{n1} completed chains written to MySQL DB ({n2} are unique)'.format(n1=n1, n2=n2))

chains_printout_path = os.path.join(experiment_path, 'chains.txt')
chains_str = '\n'.join(built_chains)
with open(chains_printout_path, 'w') as f: f.write(chains_str)

# An indirect way of identifying built chains, as the pipeline does not seem to write them

chains_with_terminals = []
terminals_in_chains = []

for chain in built_chains:
	chain = chain.split(node_delimiter)
	chain_terminal = chain[-1]
	if chain_terminal in terminal_nodes:
		chains_with_terminals.append(node_delimiter.join(chain))
		terminals_in_chains.append(chain_terminal)
terminals_in_chains_count = pd.Series(terminals_in_chains).value_counts()
tp_df = \
	pd.DataFrame(list(zip(terminals_in_chains_count.index, terminals_in_chains_count.values)), \
	             columns=['terminal', 'count'])
terminals_predecessors = []
for terminal in tp_df['terminal']:
	terminal_predecessors = list(G.predecessors(terminal))
	terminals_predecessors.append(len(terminal_predecessors))
tp_df['predecessors'] = terminals_predecessors
#print(tp_df)
tp_df.to_excel(os.path.join(experiment_path, 'terminals_predecessors.xlsx'), index=False)

chains_with_terminals = list(set(chains_with_terminals))
print('The indirect method retrieved {n3} built chains as chains with terminal nodes'.format(
	n3=len(chains_with_terminals)))
chains_printout_path = os.path.join(experiment_path, 'built_chains_indirect.txt')
chains_with_terminals = '\n'.join(chains_with_terminals)
with open(chains_printout_path, 'w') as f: f.write(chains_with_terminals)
del chains_with_terminals


# Chains pairs in source successor-predecessor pairs
print('Chains pairs in source successor-predecessor pairs')
pairs = []
for index, chain in enumerate(built_chains):
	chain = chain.split(node_delimiter)
	chain_pairs = []
	# Remove the first "node" that is actually the chain hash code (as in ('-6647253805722511403', 'MWH06-C1.P1900'))
	if len(chain) > 2:
		chain = chain[1:]
		for index, id in enumerate(chain):
			if index < len(chain)-1:
				pair = (id, chain[index+1])
				if pair not in pairs: pairs.append(pair)
pairs = list(set(pairs))
pairs_in_source = list(set([p for p in pairs if p in source_pairs]))
pairs_not_in_source = list(set([p for p in pairs if p not in source_pairs]))
n1, n2 = len(pairs), len(pairs_in_source)
error_pairs_count = n1 - n2
error_pairs_perc = round(100*error_pairs_count/n1, 2)
print('The chains produced are built of {n1} successor-predecessor pairs, of which {n2} appear as such at the program file'
      .format(n1=n1, n2=n2))
print('Pairs errors count={n} | rate={r}%'.format(n=error_pairs_count, r=error_pairs_perc))

error_pairs = list(set(pairs).difference(set(pairs_in_source)))
pairs_strs = ['<>'.join(p) for p in error_pairs]
# Error chains
errors_count, errors, errors_counts = 0, [], []
for index, chain in enumerate(built_chains):
	chain_errors_count = 0
	chain_errors = []
	for pair in pairs_strs:
		if pair in chain:
			chain_errors.append(pair)
	if chain_errors:
		chain_errors_count = len(chain_errors)
		chain_errors = ','.join(chain_errors)
		errors_count += 1
	else:
		chain_errors = ''
	errors.append(chain_errors)
	errors_counts.append(chain_errors_count)

chains_count = len(built_chains)
errors_perc = round(100*errors_count/chains_count, 2)

print('Successor-Predecessor errors identified in {n1} of {n2} built chains'.format(n1=errors_count, n2=chains_count))
print('Chains errors count={n} | rate={r}%'.format(n=errors_count, r=errors_perc))

# Count terminal nodes reached
chains_lists = [c.split(node_delimiter) for c in built_chains]
chains_terminals = list(set([c[-1] for c in chains_lists]))
terminals_in_chains = [t for t in terminal_nodes if t in chains_terminals]
print('{n1} of {n2} terminal nodes reached by chains'\
      .format(n1=len(terminals_in_chains), n2=len(terminal_nodes)))
del chains_lists

