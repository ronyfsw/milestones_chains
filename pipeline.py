from modules.worm_modules import *
start_time = datetime.now().strftime("%H:%M:%S")
print('pipeline started on', start_time)
parser = argparse.ArgumentParser()
parser.add_argument('path')
args = parser.parse_args()
graph_path = args.path
file = graph_path.split('/')[-1].replace('.edgelist', '')
pid = os.getpid()
# Initialized the next journey steps
terminal_nodes = open('terminal_nodes.txt').read().split('\n')
G = nx.read_edgelist(graph_path, create_using=nx.DiGraph)
subG_terminal_nodes = get_terminal_nodes(G)
a = set(terminal_nodes).difference((set(subG_terminal_nodes)))
Gsort = list(nx.topological_sort(G))
root_node = Gsort[0]
root_edges = [e for e in G.edges() if root_node in e]
root_successors = tuple(G.successors(root_node))
redisClient.hset('scaffolds', 1, root_node)
next_journeys_steps = [(1, root_successors)]
growth_tip = ['no tip']
chains_results_rows = []
journey = chains_written_count = 0
while next_journeys_steps:
    # Journey tracker values initiation
    journey_chains_count = overlap_count = 0
    steps_chunk = next_journeys_steps[:journey_chunk]
    next_journeys_steps = next_journeys_steps[journey_chunk:]
    steps_produced, maps_produced = [], []
    journey += 1
    next_count = len(steps_chunk)
    executor = ProcessPoolExecutor(available_executors)
    if next_count <= available_executors:
        executor = ProcessPoolExecutor(next_count)
    # Build chains
    
    ids_chains = []
    for cid, chain, next_steps in executor.map(growReproduce, steps_chunk):
        if cid:
            ids_chains.append((cid, chain))
            steps_produced += next_steps
    # Write chain scaffolds
    for cid_chain in ids_chains:
        cid, chain = cid_chain
        # Update chains
        growth_tip = chain.split(node_delimiter)[-1]
        if growth_tip in terminal_nodes:
            chains_results_rows.append((cid, chain))
        # Update scaffolds
        else:
            redisClient.hset('scaffolds', cid, chain)
    
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
    # Write chains
    if len(chains_results_rows) > 0:
        statement = insert_rows(db_name, chains_table, chains_cols, chains_results_rows)
        cur.execute(statement)
        conn.commit()
        journey_chains_count = len(chains_results_rows)
        chains_written_count += journey_chains_count
        chains_results_rows = []
    # Collect and prepare next journey steps
    next_journeys_steps = next_journeys_steps + steps_produced + maps_produced
    # filter saturated scaffolds
    scaffolds_count = redisClient.hlen('scaffolds')
    next_journeys_steps_count = len(next_journeys_steps)
    print('file:{f}|pid:{p}|journey {g}|{n} steps|{n1} scaffolds|{n2} next journeys steps|{n3} journey chains|{n4} chains'
          .format(f=file, p=pid, g=journey, n=next_count, n1=scaffolds_count, n2=next_journeys_steps_count,\
                  n3=journey_chains_count, n4=chains_written_count))
# Write the remaning results
if chains_results_rows:
    statement = insert_rows(db_name, chains_table, chains_cols, chains_results_rows)
    cur.execute(statement)
    conn.commit()
print('{p} finished'.format(p=pid))
print('pipeline started on', start_time)
print('pipeline ended on', datetime.now().strftime("%H:%M:%S"))