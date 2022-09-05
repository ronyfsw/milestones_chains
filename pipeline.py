from modules.worm_modules import *
from modules.chains import *
start_time = datetime.now().strftime("%H:%M:%S")
print('pipeline started on', start_time)
parser = argparse.ArgumentParser()
parser.add_argument('path')
args = parser.parse_args()
graph_path = args.path
file = graph_path.split('/')[-1].replace('.edgelist', '')
pid = os.getpid()

# Tasks and Links
file_path = os.path.join(data_path, data_file_name)
G = build_graph(file_path)
links = G.edges(data=True)
links_types = {}
for link in links: links_types[(link[0], link[1])] = link[2]['Dependency']
tasks_decoder = np.load('nodes_decoder.npy', allow_pickle=True)[()]
metadata_duration = pd.read_excel('metadata_duration.xlsx')

# Initialized the next journey steps
terminal_nodes = open('terminal_nodes.txt').read().split('\n')
G = nx.read_edgelist(graph_path, create_using=nx.DiGraph)
Gsort = list(nx.topological_sort(G))
root_node = Gsort[0]
root_successors = tuple(G.successors(root_node))
redisClient.hset('scaffolds', 1, root_node)
next_journeys_steps = [(1, root_successors)]
growth_tip = ['no tip']
executor = ProcessPoolExecutor(available_executors)
journey = chains_written_count = tasks_written_count = 0

while next_journeys_steps:
    journey_chains_count = journey_tasks_count = 0
    chains_results = []
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
            chains_results.append((cid, chain))
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
    if len(chains_results) > 0:
        chains_rows = []
        # chains_results element = (cid, chain)
        for id_chain in chains_results:
            tasks = id_chain[1].split(node_delimiter)
            tasks = [tasks_decoder[t] for t in tasks]
            id_chain = (id_chain[0], tasks)
            chain_rows = chain_to_rows(id_chain, links_types)
            chains_rows += chain_rows
        del chains_results    
        chains_rows = pd.DataFrame(chains_rows, columns=['ID', 'ChainID', 'NeighbourID', 'Dependency'])
        # print('chains_rows')
        # print(chains_rows.head())
        # print(chains_rows.info())
        tasks = pd.merge(chains_rows, metadata_duration, how='inner')
        # print('tasks')
        # print(tasks.head())
        # print(tasks.info())
        tasks.to_sql(results_table, engine, index=False, if_exists='append')
        results_conn.commit()
        #statement = insert_rows(db_name, chains_table, chains_cols, chains_rows)
        #cur.execute(statement)
        #conn.commit()
        journey_chains_count = len(chains_rows)
        chains_written_count += journey_chains_count
        journey_tasks_count = len(tasks)
        tasks_written_count += journey_tasks_count

    # Collect and prepare next journey steps
    next_journeys_steps = next_journeys_steps + steps_produced + maps_produced
    # filter saturated scaffolds
    scaffolds_count = redisClient.hlen('scaffolds')
    next_journeys_steps_count = len(next_journeys_steps)
    # print('file:{f}|pid:{p}|journey {g}|{n} steps|{n1} scaffolds|{n2} next journeys steps|{n3} journey chains|{n4} chains'\
    #       .format(f=file, p=pid, g=journey, n=next_count, n1=scaffolds_count, n2=next_journeys_steps_count,\
    #               n3=journey_chains_count, n4=chains_written_count))
    print('file:{f}|pid:{p}|journey {g}|{n} steps|{n1} scaffolds|{n2} next journeys steps|{n3} journey chains|{n4} chains|journey|{tr} tasks |{tt} journeys tasks' \
    .format(f=file, p=pid, g=journey, n=next_count, n1=scaffolds_count, n2=next_journeys_steps_count, \
              n3=journey_chains_count, n4=chains_written_count, tr=journey_tasks_count, tt=tasks_written_count))

# Write the remaning results
if len(chains_rows)>0:
    chains_rows = pd.DataFrame(chains_rows, columns=['ID', 'ChainID', 'NeighbourID', 'Dependency'])
    tasks = pd.merge(chains_rows, metadata_duration, how='inner')
    tasks.to_sql(results_table, engine, index=False, if_exists='append')
    results_conn.commit()
    # statement = insert_rows(db_name, chains_table, chains_cols, chains_rows)
    # cur.execute(statement)
    # conn.commit()
print('{p} finished'.format(p=pid))
print('pipeline started on', start_time)
print('pipeline ended on', datetime.now().strftime("%H:%M:%S"))