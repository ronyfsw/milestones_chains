from modules.config import *
from modules.libraries import *
from modules.graphs import *
from modules.chains import *
from modules.encoders import *
from modules.filters import *
from modules.worm_modules import *
import warnings
warnings.filterwarnings("ignore")

## Data
print(file_path)
G = build_graph(file_path)
Gnodes, Gedges = list(G.nodes()), G.edges()
print(Gnodes)
print('Graph with {n} nodes and {e} edges'.format(n=len(Gnodes), e=len(Gedges)))
isolates = graph_isolates(G)
terminal_nodes = get_terminal_nodes(G)
print('terminal nodes:', terminal_nodes)
root_node = list(nx.topological_sort(G))[0]

# Encode nodes
nodes_encoder = objects_encoder(Gnodes)
nodes_decoder = build_decoder(nodes_encoder)
np.save('nodes_decoder.npy', nodes_decoder)
G = nx.relabel_nodes(G, nodes_encoder)
Gnodes, Gedges = list(G.nodes()), G.edges()
isolates = [nodes_encoder[n] for n in isolates]
terminal_nodes = [nodes_encoder[n] for n in terminal_nodes]
print('terminal nodes:', terminal_nodes)
root_node = nodes_encoder[root_node]

# Results tables
c.execute("DROP TABLE IF EXISTS {t}".format(t=tracker_table))
statement = build_create_table_statement('{t}'.format(t=tracker_table), tracker_cols_types)
c.execute(statement)

# Initialized crawling and worms parameters
certificate = (0, root_node, list(G.successors(root_node))[0])

## Walk ##
# Termination condition: all worms reached terminal nodes
growth_certificates_index, birth_certificates_index = 0, 0
certificates = {'growth_certificates': [], 'birth_certificates': [certificate]}

gc_applied, bc_applied = [], []
step = 0
tracker_rows = []
# todo chains_built to dictionary
# chains_built = [(0, root_node)]
chains_build_db.set(0, root_node)
chains_results_rows = []
cert_chains = []

errors_counter = 0
start_process = time.time()
chains_built = []
monitor_rows = []
birth_certificates, growth_certificates = [], []
while certificate:
	error_pairs = []
	step_start = time.time()
	growth_tip = ['no tip']
	step += 1
	chain_built=new_chain=applied_certificates_count=\
    chains_count=start_duration=growth_duration=prep_chain_duration=write_duration=reproduce_duration=\
    update_duration=certificate_select_duration = 0
	start_duration = round(time.time() - step_start, 3)

	# Worm/Chain growth
	start = time.time()
	walk = wormWalk(G, certificate)
	if len(certificate) == 2:
		gc_applied.append(certificate)
	else: bc_applied.append(certificate)

	chainIndex, chain = walk.grow()
	growth_duration = round(time.time() - start, 3)
	start = time.time()
	if chain:
		# Assert and prepare new chain
		chain_built = 1
		growth_tip = chain[-1]
		#growth_certificates.append((chainIndex, growth_tip))
		chains_count = len(chains_build_db.keys())
		chain_str = node_delimiter.join([n.rstrip().lstrip() for n in chain])
		certificate_nodes = list(certificate)[2:]
		for node in certificate_nodes:
			if node in isolates: print(node, 'is an isolate')
			if node in terminal_nodes: print(node, 'is a terminal node')
		assert_duration = round(time.time() - start, 3)
		assert_duration = round(time.time() - start, 3)
		growth_certificates.append((chainIndex, growth_tip))
		start = time.time()
		#chains_kv = {}
		#for k, v in chains_built_db.keys(): chains_kv[k] = v
		if growth_tip in terminal_nodes:
			decoded = decode_chains([chain_str], nodes_decoder)
			chains_built_db.set(chainIndex, chain_str)
		else:
			chains_build_db.set(chainIndex, chain_str)
		chains_count = len(chains_build_db.keys())
		write_duration = round(time.time() - start, 3)

		# Update birth_certificates with decendants' birth certificates
		start = time.time()
		birth_certificates += walk.reproduce(growth_tip)
		reproduce_duration = round(time.time() - start, 3)

	# Update birth and growth certfiicates
	start = time.time()
	growth_certificates = list(set(binarySearchFilter(growth_certificates, gc_applied)))
	birth_certificates = list(set(binarySearchFilter(birth_certificates, bc_applied)))
	growth_certificates = [gc for gc in growth_certificates if gc[1] not in terminal_nodes]
	update_duration = round(time.time() - start, 3)

	print(step)
	# print(step, certificate, chain)
	# print(30 * '=')

	# Next iteration certificate
	start = time.time()
	certificate_applied = certificate
	monitor_row = [step, certificate_applied, chain, str(growth_certificates), str(birth_certificates)]
	if growth_certificates:
		certificate = growth_certificates[0]
		growth_certificates.pop(0)
		a = 0
	#if growth_tip in terminal_nodes:
	elif birth_certificates:
		certificate = birth_certificates[0]
		birth_certificates.pop(0)
		b = 1
	certificate_select_duration = time.time()-start

	# Duration tracking
	step_duration = round(time.time()-step_start, 3)
	processes_duration_vals = [start_duration, growth_duration, prep_chain_duration, write_duration, assert_duration, reproduce_duration, \
	                                                                                         update_duration, certificate_select_duration]
	processes_duration = sum(processes_duration_vals)
	diff = round(step_duration-processes_duration, 3)
	applied_certificates_count = len(gc_applied) + len(bc_applied)
	tracker_row = [step, chain_built, new_chain, applied_certificates_count, \
                   chains_count, start_duration, growth_duration, prep_chain_duration, write_duration, assert_duration, reproduce_duration,\
                   update_duration, certificate_select_duration, processes_duration, step_duration, diff]
	statement = insert_row('{db}.{tt}'.format(db=db_name, tt=tracker_table), list(tracker_cols_types.keys()), tracker_row)
	c.execute(statement)
	conn.commit()
