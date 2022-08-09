import mysql.connector as mysql_con
from modules_delete.utils import *
from modules_delete.splitgraph import *

user, password, database = 'rony', 'exp8546$fs', 'MCdb'
conn_params = {'host': 'localhost', 'user': user, 'password': password, 'database': database}
conn = mysql_con.connect(**conn_params, allow_local_infile = True)
c = conn.cursor()

def milestone_nodes(G, include_fin = True):
	'''
	:param G: Graph object
	:return: The nodes for the milestones in the input graph
	'''
	G_nodes, G_edges = G.nodes(), G.edges()
	reg_milestone_ids = [node for node in G_nodes if G_nodes[node]['TaskType'] == 'TT_Mile']
	if include_fin:
		fin_milestone_ids = [node for node in G_nodes if G_nodes[node]['TaskType'] == 'TT_FinMile']
		milestone_ids = reg_milestone_ids + fin_milestone_ids
	milestones = {}
	for milestone_id in milestone_ids: milestones[milestone_id] = G_nodes[milestone_id]
	return milestones

def milestones_pairs_duration(milestone_paths, planned_duration, ids_names):
	milestones_duration = {}
	global milestones_pair_duration

	def milestones_pair_duration(pair_tasks):
		milestones_pair, inter_milestone_tasks = pair_tasks
		milestone_duration = 0
		tasks_duration = {}
		for task_id in inter_milestone_tasks:
			task_duration = planned_duration[task_id]
			milestone_duration += task_duration
			name_duraion = (ids_names[task_id], task_duration)
			tasks_duration[task_id] = name_duraion
		duration_dict = {'milestone_duration': milestone_duration, 'tasks_duration': tasks_duration}
		return pair_tasks, duration_dict

	pairs_tasks = []
	for milestones_pair, inter_milestone_tasks in milestone_paths.items():
		pairs_tasks.append((milestones_pair, inter_milestone_tasks))

	executor = ProcessPoolExecutor(4)
	for pair_tasks, duration_dict in executor.map(milestones_pair_duration, pairs_tasks):
		milestones_pair = pair_tasks[0]
		milestones_duration[milestones_pair] = duration_dict

	return milestones_duration

from datetime import datetime
def milestones_achievement_duration(chain, metadata_df):

	'''
	Calculate the achivement duration for the milestones in a milestone chain
	Milestone Planned/Actual Duration: Planned/Actual achievement date for this milestone -
										Planned/Actual achievement date for the previous milestone.
	The start and end dates for each milestone in the chain,\
	using either start or end dates as they are the same for milestone tasks
	For the first milestone in the chain the value is NA.
	:param metadata_df(DataFrame): The tasks metadata as extracted from a graphml file
	:param chain (list): The tasks in a milestone path, starting from the first and ending
	in the last miletstone in the chain
	:return:
	'''
	date_format = "%d/%m/%Y"
	milestones, milestones_dates = [], {}
	for index, task_id in enumerate(chain):
		task_type = metadata_df['TaskType'][metadata_df['ID'] == task_id].values
		if task_type != 'TT_Task':
			# Collect milestones
			milestones.append(task_id)
			# Achievement Dates for milestones
			dates = tuple(metadata_df[['PlannedStart', 'ActualStart']][metadata_df['ID'] == task_id].values[0])
			milestones_dates[task_id] = dates
	
	# Calculate milestone duration values
	planned_duration_ms, actual_duation_ms = {}, {}
	for index, task_id in enumerate(milestones):
		# Values for the first milestone
		if index == 0: planned_duration, actual_duration = None, None
		else:
			m1, m2 = milestones[index-1], milestones[index]
			plannedm1, plannedm2 = datetime.strptime(milestones_dates[m1][0], date_format), \
			                       datetime.strptime(milestones_dates[m2][0], date_format)
			actualm1, actualm2 = datetime.strptime(milestones_dates[m1][1], date_format), \
			                       datetime.strptime(milestones_dates[m2][1], date_format)
			planned_duration = (plannedm2 - plannedm1).days
			actual_duration = (actualm2 - actualm1).days
		planned_duration_ms[task_id] = planned_duration
		actual_duation_ms[task_id] = actual_duration

	# Add None for TDAs
	results = []
	for index, task_id in enumerate(chain):
		task_type = metadata_df['TaskType'][metadata_df['ID'] == task_id].values
		if task_type == 'TT_Task':
			planned_duration, actual_duration = None, None
		else:
			planned_duration, actual_duration = planned_duration_ms[task_id], \
			                                    actual_duation_ms[task_id]
		results.append((task_id,planned_duration, actual_duration))

	results_df = pd.DataFrame(results, columns=['ID', 'Planned_MS_Duration', 'Actual_MS_Duration'])
	results_df = pd.merge(results_df, metadata_df, how='left')
	return results_df

def is_milestones_chain(chain_ids_types, milestone_types=['TT_Mile', 'TT_FinMile']):
	'''
	Identify a task chain as milestones chains (starts and end in a milestone)
	based on the identification of chain tasks as milestones
	:param chain (list): A sequence of tasks
	:param ids_types (dictionary): Graph tasks types keyed by their task ids
	:return:
	'''
	chain, ids_types = chain_ids_types
	confirm = False
	start_id, end_id = chain[0], chain[-1]
	start_type, end_type = ids_types[start_id], ids_types[end_id]
	if ((any(start_type == t for t in milestone_types)) &
			(any(end_type == t for t in milestone_types))):
		confirm = True
	chain = [str(t) for t in chain]
	return chain, confirm

def handle_link_type(chain, remove=False):
	'''
	Return or remove the type of nodes link from the chain representation of the nodes
	:param chain (list): A representation of the node pair in the form of [node 1, link, node2]
	for example: ['MWH.06.M1000', '<FS>', 'MWH06-2029']
	:param remove (bool): Instruction to remove the chain link
	:return: The chain link (default) or the chain without the link(remove=True)
	'''
	dependency_types = ['FS', 'SF', 'SS', 'FF']
	if remove: return [i for i in chain if not any(t in i for t in dependency_types)]
	else:
		m = [i for i in chain if any(t in str(i) for t in dependency_types)]
		if m: return m[0]
		else: return ''

def get_tasks_types(chains, ids_types):
	chain_links = [handle_link_type(chain) for chain in chains]
	chains_tasks_types = []
	for index, chain in enumerate(chains):
		chain_link = chain_links[index]
		chain_nodes = [n for n in chain if n != chain_link]
		nodes_types = {k: v for k, v in ids_types.items() if k in chain_nodes}
		chains_tasks_types.append((chain, nodes_types))
	return (chains_tasks_types)


def build_ids_types(G):
	'''
	Map task id to task type for each task in an input graph
	:param G: An input graph object
	:return:
	'''
	tasks_types = list(nx.get_node_attributes(G, "TaskType").values())
	Gnodes = list(G.nodes())
	ids_types = dict(zip(Gnodes, tasks_types))
	return ids_types

def get_successors_links(node, successors, edges_types):
	'''
	Identify the types of links between a node and each of its successors
	:param node (list): A graph node name
	:param successors(list): The node's successors
	:param edges_types(dict): The list of link types per edge keyed by a tuple of the nodes connected by this edge
	:return: A dictionary of link types per successor keyed by (node, successor) tuple
	'''
	successors_links = {}
	for successor in successors:
		pair = (node, successor)
		successors_links[successor] = edges_types[pair]
	return successors_links


def tasks_hash_to_ids(chains, hash_nodes_map):
	ids_chains = []
	for chain in chains:
		chain_ids = [hash_nodes_map[float(task)] for task in chain]
		ids_chains.append(chain_ids)
	return ids_chains

def select_milestone_chains(chains, num_executors, tmp_path, ids_types, hash_nodes_map, conn):

	# todo: replace drop milestone_chains by creating a results table indexed by file name or id
	c.execute("DROP TABLE IF EXISTS milestone_chains")
	c.execute("CREATE TABLE IF NOT EXISTS milestone_chains (chain varchar(255))")


	# Identify the type of each task in the chains identified
	chains_tasks_types = get_tasks_types(chains, ids_types)
	## Filter tasks chains
	milestone_chains = []
	# Parallelized
	executor2 = ProcessPoolExecutor(num_executors)
	for chain2, confirm in executor2.map(is_milestones_chain, chains_tasks_types):
	# for chain2, confirm in map(is_milestones_chain, chains_tasks_types):
		if confirm: milestone_chains.append(chain2)
	executor2.shutdown()
	milestone_chains = tasks_hash_to_ids(milestone_chains, hash_nodes_map)
	milestone_chains = chains_to_strings(milestone_chains)
	## Write results: milestone chains
	write_chains(milestone_chains, 'milestone_chains', tmp_path, conn, convert=False)

	return milestone_chains

