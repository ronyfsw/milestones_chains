from modules.config import *
from modules.encoders import *
from modules.nodes import *
from modules.filters import *
from modules.worm_modules import *
def run_pipeline(Gindex, G):
    pid = os.getpid()
    # Initialized the next journey steps
    terminal_nodes = open('terminal_nodes.txt').read().split('\n')
    # subG_terminal_nodes = get_terminal_nodes(G)
    # a = set(terminal_nodes).difference((set(subG_terminal_nodes)))
    Gsort = list(nx.topological_sort(G))
    root_node = Gsort[0]
    print('process: {p} | graph: {g} | root: {r}'\
          .format(p=pid,  g=G, r=root_node))
    root_edges = [e for e in G.edges() if root_node in e]
    print('root_edges:', root_edges)
    root_successors = tuple(G.successors(root_node))
    print('root_successors:', root_successors)
    redisClient.hset('scaffolds', 1, root_node)
    next_journeys_steps = [(1, root_successors)]
    print('next_journeys_steps:', next_journeys_steps)

    chains_results_rows = []
    journey = chains_written_count = 0

    while next_journeys_steps:
        # Journey tracker values initiation
        journey_chains_count = overlap_count = 0
        journey_start = time.time()
        #steps_chunk = next_journeys_steps[:journey_chunk]
        #next_journeys_steps = next_journeys_steps[journey_chunk:]
        steps_produced, maps_produced = [], []
        journey += 1
        next_count = len(next_journeys_steps)
        print(60 * '*')
        print('***** journey {g}: {n} steps | pid: {p}*****'.format(g=journey, n=next_count, p=pid))

        executor = ProcessPoolExecutor(available_executors)
        if next_count <= available_executors:
            executor = ProcessPoolExecutor(next_count)

        # Build chains
        start = time.time()
        ids_chains = []
        for cid, chain, next_steps in executor.map(growReproduce, next_journeys_steps):
            if cid:
                ids_chains.append((cid, chain))
                steps_produced += next_steps
        grow_reproduceD = round(time.time() - start, 2)

        # Identify none-unique IDs
        start = time.time()
        # todo: replace overlap checkpoint by extending id to include pid
        # db_keys = list(redisClient.hkeys('scaffolds'))
        # ids_chains = checkReviseKeysOverlap(ids_chains, db_keys)
        unique_idsD = round(time.time() - start, 2)

        # Write chain scaffolds
        start = time.time()
        for cid_chain in ids_chains:
            cid, chain = cid_chain
            # Update chains
            growth_tip = chain.split(node_delimiter)[-1]
            if growth_tip in terminal_nodes:
                chains_results_rows.append((cid, chain))
            # Update scaffolds
            else:
                redisClient.hset('scaffolds', cid, chain)
        write_scaffoldsD = round(time.time() - start, 2)

        start = time.time()
        # Update maps
        ids = [i[0] for i in ids_chains]
        chains = [i[1] for i in ids_chains]
        growth_tips = [chain.split(node_delimiter)[-1] for chain in chains]
        journey_chains_count = len(ids_chains)
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
        write_chainsD = round(time.time() - start, 2)
        # Collect and prepare next journey steps

        start = time.time()
        next_journeys_steps = next_journeys_steps + steps_produced + maps_produced
        next_stepsD = round(time.time() - start, 2)

        # filter saturated scaffolds
        scaffolds_count = redisClient.hlen('scaffolds')
        journeyD = round(time.time() - journey_start, 2)
        print('journey=', journeyD)
        chains_written_count = len(chains_results_rows)
        print('{n1} next journey scaffolds | {n2} journey chains | {n3} sub graph chains'
              .format(n1=scaffolds_count, n2=journey_chains_count, n3=chains_written_count))

        # Write tracker values
        tracker_row = [journey, next_count, scaffolds_count, \
                       journey_chains_count, chains_written_count, \
                       overlap_count, grow_reproduceD, unique_idsD, \
                       write_scaffoldsD, update_mapsD, \
                       write_chainsD, next_stepsD, journeyD]
        #print(tracker_row)
        statement = insert_row('{db}.{tt}'.format(db=db_name, tt=tracker_table), list(tracker_cols_types.keys()),
                               tracker_row)
        cur.execute(statement)
        conn.commit()
    return chains_results_rows