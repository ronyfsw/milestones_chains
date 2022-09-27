from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from libraries import *
from db_tables import *
from chains import *
from config import *

start_time = datetime.now().strftime("%H:%M:%S")
pid = os.getpid()
print('build chains process {p} started on'.format(p=pid), start_time)

parser = argparse.ArgumentParser()
parser.add_argument('sub_graph_file_name')
parser.add_argument('experiment')
parser.add_argument('tasks_types')
parser.add_argument('results')
args = parser.parse_args()
# print('args:', args)
sub_graph_file_name = args.sub_graph_file_name
experiment = args.experiment
tasks_types = args.tasks_types
results = args.results
chains_table = '{e}_chains'.format(e=experiment)

# Tasks
tasks_decoder = np.load(os.path.join(run_dir_path, 'nodes_decoder.npy'), allow_pickle=True)[()]
terminal_nodes = open(os.path.join(run_dir_path,'terminal_nodes.txt')).read().split('\n')
nodes_types = np.load(os.path.join(run_dir_path, 'nodes_types.npy'), allow_pickle=True)[()]
G = nx.read_edgelist(sub_graph_file_name, create_using=nx.DiGraph)
Gsort = list(nx.topological_sort(G))
root_node = Gsort[0]
root_successors = tuple(G.successors(root_node))
scaffolds = {1: root_node}
scaffolds_dict = os.path.join(scaffolds_path, 'scaffolds_{p}.npy'.format(p=pid))
np.save(scaffolds_dict, scaffolds)
next_journeys_steps = [(pid, 1, root_successors)]
growth_tip = ['no tip']
executor = ProcessPoolExecutor(available_executors)
journey = chains_written_count = tasks_written_count = 0
chains_rows = []
while next_journeys_steps:
    # Journey tracker values initiation
    journey_chains_count = overlap_count = journey_tasks_count = 0
    steps_chunk = next_journeys_steps[:journey_chunk]
    next_journeys_steps = next_journeys_steps[journey_chunk:]

    steps_produced, maps_produced = [], []
    journey += 1
    next_count = len(steps_chunk)
    if next_count <= available_executors:
        executor = ProcessPoolExecutor(next_count)

    # Build chains
    ids_chains = []
    for cid, chain, next_steps in executor.map(growReproduce, steps_chunk):
        if cid:
            ids_chains.append((cid, chain))
            steps_produced += next_steps

    # Collect chains and write scaffolds
    for cid_chain in ids_chains:
        cid, chain = cid_chain
        # Update chains
        growth_tip = chain.split(node_delimiter)[-1]
        if growth_tip in terminal_nodes:
            chains_rows.append((cid, chain))
        # Update scaffolds
        else:
            scaffolds[cid] = chain
    np.save(scaffolds_dict, scaffolds)

    # Update maps
    ids = [i[0] for i in ids_chains]
    chains = [i[1] for i in ids_chains]
    growth_tips = [chain.split(node_delimiter)[-1] for chain in chains]
    del ids_chains
    for index, tip in enumerate(growth_tips):
        growth_tip_successors = successorsDB.get(tip)
        if growth_tip_successors:
            growth_tip_successors = tuple(growth_tip_successors.split(','))
            maps_produced.append((pid, ids[index], growth_tip_successors))
    del ids

    # Write chains
    if len(chains_rows) > 0:
        statement = insert_rows(db_name, chains_table, chains_cols, chains_rows)
        cur.execute(statement)
        conn.commit()
        journey_chains_count = len(chains_rows)
        chains_written_count += journey_chains_count
        chains_rows = []

    # Collect and prepare next journey steps
    next_journeys_steps = next_journeys_steps + steps_produced + maps_produced

    # filter saturated scaffolds
    scaffolds_count = len(scaffolds)
    next_journeys_steps_count = len(next_journeys_steps)

# Write the remaning results
if len(chains_rows) > 0:
    statement = insert_rows(db_name, chains_table, chains_cols, chains_rows)
    cur.execute(statement)
    conn.commit()
conn.close()
print('build chains {p} ended on'.format(p=pid), datetime.now().strftime("%H:%M:%S"))