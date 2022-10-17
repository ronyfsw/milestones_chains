import re
from pathlib import Path
import os
import sys
import numpy as np

home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from libraries import *
from chains import *
from config import *
from db_tables import *

start_time = datetime.now().strftime("%H:%M:%S")
print('pipeline started on', start_time)

# Run arguments
parser = argparse.ArgumentParser()
parser.add_argument('sub_graph_file_path')
parser.add_argument('experiment')
args = parser.parse_args()
sub_graph_file_path = args.sub_graph_file_path
experiment = args.experiment
chains_table = '{e}_chains'.format(e=experiment)

# Subgraph data
G = nx.read_edgelist(sub_graph_file_path, create_using=nx.DiGraph)
Gsort = list(nx.topological_sort(G))
root_node = Gsort[0]
root_successors = tuple(G.successors(root_node))
terminal_nodes = open(os.path.join(run_dir_path, 'terminal_nodes.txt')).read().split('\n')

# Initialize the next journey steps
pid = os.getpid()
redisClient.hset('scaffolds', 1, root_node)
next_journeys_steps = [(pid, 1, root_successors)]
growth_tip = ['no tip']
executor = ProcessPoolExecutor(available_executors)
journey = chains_written_count = tasks_written_count = 0
chains_rows = []
while next_journeys_steps:
    journey_chains_count = journey_tasks_count = 0
    # Journey tracker values initiation
    journey_chains_count = overlap_count = 0
    steps_chunk = next_journeys_steps[:journey_chunk]
    next_journeys_steps = next_journeys_steps[journey_chunk:]

    steps_produced, maps_produced = [], []
    journey += 1
    next_count = len(steps_chunk)
    if next_count <= available_executors:
        executor = ProcessPoolExecutor(next_count)

    # Build chains
    ids_chains = []
    print('*** steps_chunk:', steps_chunk)
    for cid, chain, next_steps in executor.map(growReproduce_redis, steps_chunk):
        if cid:
            ids_chains.append((cid, chain))
            steps_produced += next_steps

    # Write chain scaffolds
    for cid_chain in ids_chains:
        cid, chain = cid_chain
        # Update chains
        growth_tip = chain.split(node_delimiter)[-1]
        if growth_tip in terminal_nodes:
            chains_rows.append((cid, chain))
        # Update scaffolds
        else:
            redisClient.hset('scaffolds', cid, chain)

    # Update maps redis version
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
    scaffolds_count = redisClient.hlen('scaffolds')
    next_journeys_steps_count = len(next_journeys_steps)
    print('{n1} scaffolds, {n2} next journeys steps, {n3} journey chains, {n4} chains'
          .format(n1=scaffolds_count, n2=next_journeys_steps_count, n3=journey_chains_count, n4=chains_written_count))

# Write the remaining results
if len(chains_rows) > 0:
    statement = insert_rows(db_name, chains_table, chains_cols, chains_rows)
    cur.execute(statement)
    conn.commit()
    journey_chains_count = len(chains_rows)
    chains_written_count += journey_chains_count
    chains_rows = []

print('{p} finished'.format(p=pid))
print('pipeline started on', start_time)
print('pipeline ended on', datetime.now().strftime("%H:%M:%S"))