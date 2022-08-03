import time

with open('terminal_nodes.txt', 'w') as f: f.write('tmp')
from modules.config import *
from modules.libraries import *
from modules.graphs import *
from modules.chains import *
from modules.encoders import *
from modules.nodes import *
from modules.filters import *
from modules.worm_modules import *
import warnings
warnings.filterwarnings("ignore")

# Data
G = build_graph(file_path)
Gnodes, Gedges = list(G.nodes()), G.edges()
terminal_nodes = get_terminal_nodes(G)
with open('terminal_nodes.txt', 'w') as f: f.write('\n'.join(terminal_nodes))
# Encode nodes
nodes_encoder = objects_encoder(Gnodes)
nodes_decoder = build_decoder(nodes_encoder)
np.save('nodes_encoder.npy', nodes_encoder)
np.save('nodes_decoder.npy', nodes_decoder)
G = nx.relabel_nodes(G, nodes_encoder)
Gnodes, Gedges = list(G.nodes()), G.edges()

# for Gnode in Gnodes:
# 	successorsDB.set(Gnode, ','.join(list(G.successors(Gnode))))

isolates = graph_isolates(G)
saturation_successors, saturation_predecessors = {}, {}
for Gnode in Gnodes:
	if Gnode not in isolates:
		node_successors, node_predecessors = list(G.successors(Gnode)), list(G.predecessors(Gnode))
		successorsDB.set(Gnode, ','.join(node_successors))
		predecessorsDB.set(Gnode, ','.join(node_predecessors))
		saturation_successors[Gnode] = node_successors
		saturation_predecessors[Gnode] = node_predecessors

print('Graph with {n} nodes and {e} edges'.format(n=len(Gnodes), e=len(Gedges)))
isolates = graph_isolates(G)
terminal_nodes = get_terminal_nodes(G)
with open('terminal_nodes.txt', 'w') as f: f.write('\n'.join(terminal_nodes))
root_node = list(nx.topological_sort(G))[0]

# Results tables
c.execute("DROP TABLE IF EXISTS {t}".format(t=tracker_table))
statement = build_create_table_statement('{t}'.format(t=tracker_table), tracker_cols_types)
c.execute(statement)

# Initialized the next journey steps
root_successors = tuple(G.successors(root_node))
redisClient.hset('scaffolds', 1, root_node)
next_journeys_steps = [(1, root_successors)]

rkeys = redisClient.hkeys('scaffolds')
rkeys_vals = redisClient.hgetall('scaffolds')
journey = 0
growth_tip = ['no tip']
steps_walked = []
chains_results_rows = []
chains_written_count = 0
#while next_journeys_steps:
journey = scaffolds_count = chains_written_count = 0
filter_round = 1
chars = list('abcdefghijklmnopqrstuvwxyz')
while next_journeys_steps:
	# Journey tracker values initiation
	next_count = scaffolds_count = filtered_scaffolds_count = \
		journey_chains_count =  grow_reproduceD =\
		overlap_count = unique_idsD = write_scaffoldsD = filter_saturatedD = \
		del_saturatedD = update_mapsD = write_chainsD = next_stepsD = journeyD = 0
	journey_start = time.time()
	journey_chains_count = 0
	steps_chunk = next_journeys_steps[:journey_chunk]
	next_journeys_steps = next_journeys_steps[journey_chunk:]
	steps_produced, maps_produced = [], []
	journey += 1
	next_count = len(steps_chunk)
	print(60*'*')
	print('***** journey {g}: {n} steps *****'.format(g=journey, n=next_count))
	step = 0
	executor = ProcessPoolExecutor(available_executors)
	if next_count <= available_executors:
		executor = ProcessPoolExecutor(next_count)

	# Build chains
	start = time.time()
	ids_chains = []
	for cid, chain, next_steps in executor.map(growReproduce, steps_chunk):
		if cid:
			random_letters = '{a}{b}{c}'.format(a=random.choice(chars), b=random.choice(chars), c=random.choice(chars))
			cid = random_letters+str(cid)
			ids_chains.append((cid, chain))
			steps_produced += next_steps
	grow_reproduceD = round(time.time()-start, 2)

	start = time.time()
	# Identify none-unique IDs
	ids = list(set([i[0] for i in ids_chains]))
	#db_keys = [float(k) for k in list(redisClient.hkeys('scaffolds'))]
	db_keys = list(redisClient.hkeys('scaffolds'))
	overlap = set(ids).intersection(set(db_keys))

	q = 0
	while overlap:
		q += 1
		print(q)
		# Revise the hash encoding for the chains with none-unique IDS
		overlap_chains = [i[1] for i in ids_chains if i[0] in overlap]
		revised_ids_chains = [(hash(chain), chain) for chain in overlap_chains]
		# Remove the chains with none-unique IDs from the chains collection
		ids_chains = [i for i in ids_chains if i not in overlap_chains]
		# Join the cleaned ids_chains list to the list of the revised ids_chains
		ids_chains = ids_chains + revised_ids_chains

		# Check overlap for the concatenated list
		ids = list(set([i[0] for i in ids_chains]))
		overlap = set(ids).intersection((db_keys))
		overlap_count = len(overlap)

	del db_keys
	print('unique_idsD=', time.time()-start)
	unique_idsD = round(time.time() - start, 2)

	# Write chain scaffolds
	start = time.time()
	for cid_chain in ids_chains:
		cid, chain = cid_chain
		# Update chains
		growth_tip = chain.split(node_delimiter)[-1]
		if growth_tip in terminal_nodes:
			chains_results_rows.append((cid, chain))
			mm = 9
		# Update scaffolds
		else:
			# chain_split = chain.split(node_delimiter)
			# if len(chain_split) > 2:
			# 	chain = [str(cid)] + chain_split[-2:]
			# 	chain = node_delimiter.join(chain)
			redisClient.hset('scaffolds', cid, chain)
	write_scaffoldsD = round(time.time() - start, 2)

	start = time.time()
	# Update maps
	ids = [i[0] for i in ids_chains]
	chains = [i[1] for i in ids_chains]
	growth_tips = [chain.split(node_delimiter)[-1] for chain in chains]
	del ids_chains
	for index, tip in enumerate(growth_tips):
		growth_tip_successors = successorsDB.get(tip)
		if growth_tip_successors:
			growth_tip_successors = tuple(growth_tip_successors.split(','))
			maps_produced.append((ids[index], growth_tip_successors))
	del ids
	update_mapsD = round(time.time() - start, 2)
	
	start = time.time()
	# Write chains
	if len(chains_results_rows) >= 100:
		statement = insert_rows('{db}.chains'.format(db=db_name), chains_cols, chains_results_rows)
		c.execute(statement)
		conn.commit()
		journey_chains_count = len(chains_results_rows)
		chains_written_count += journey_chains_count
		#print('chains_results_rows:', chains_results_rows)
		chains_results_rows = []
	print('Write chains=', time.time() - start)
	write_chainsD = round(time.time() - start, 2)
	# Collect and prepare next journey steps
	start = time.time()
	next_journeys_steps = next_journeys_steps + steps_produced + maps_produced
	next_journeys_steps = [tuple(c) for c in next_journeys_steps]
	next_journeys_steps = tuple(next_journeys_steps)
	next_journeys_steps = list(set(next_journeys_steps))
	next_journeys_steps = [list(c) for c in next_journeys_steps if len(c) > 0]
	next_stepsD = round(time.time() - start, 2)

	# filter saturated scaffolds
	scaffolds_count = redisClient.hlen('scaffolds')
	print('{n} scaffolds written'.format(n=scaffolds_count))
	# todo: condition to filters scaffolds from certain size
	if int(scaffolds_count/filter_chunk) == filter_round:
		filter_round += 1
		cids = list(redisClient.hkeys('scaffolds'))
		scaffolds = list(redisClient.hgetall('scaffolds').values())
		start = time.time()
		filter_cids = filter_scaffolds(cids, scaffolds, saturation_successors, saturation_predecessors)
		filter_saturatedD = round(time.time() - start, 2)
		print('{n} filter_cids'.format(n=len(filter_cids)))
		start = time.time()
		for cid in filter_cids: redisClient.hdel('scaffolds', cid)
		filtered_scaffolds_count = redisClient.hlen('scaffolds')
		print('{n} filtered scaffolds'.format(n=filtered_scaffolds_count))
		print('delete saturated scaffolds=', time.time() - start)
		del_saturatedD = round(time.time() - start, 2)
		#next_journeys_steps = [c for c in next_journeys_steps if str(c[0]) not in filter_cids]

	journeyD = round(time.time() - journey_start, 2)
	print('journey=', journeyD)

	if filtered_scaffolds_count > 0:
		next_journey_scaffolds = filtered_scaffolds_count
	else:
		next_journey_scaffolds = redisClient.hlen('scaffolds')
	print('{n1} next journey scaffolds | {n2} journey chains | {n3} chains'
	      .format(n1=next_journey_scaffolds, n2=journey_chains_count, n3=chains_written_count))

	# Write tracker values
	tracker_row = [journey, next_count, scaffolds_count, filtered_scaffolds_count, \
	                     journey_chains_count, chains_written_count,\
	                     overlap_count, grow_reproduceD, unique_idsD, \
	                     write_scaffoldsD, filter_saturatedD, del_saturatedD, update_mapsD, \
	                     write_chainsD, next_stepsD, journeyD]
	print(tracker_row)
	statement = insert_row('{db}.{tt}'.format(db=db_name, tt=tracker_table), list(tracker_cols_types.keys()), tracker_row)
	c.execute(statement)
	conn.commit()

# Write the remaning results
statement = insert_rows('{db}.chains'.format(db=db_name), chains_cols, chains_results_rows)
c.execute(statement)
conn.commit()