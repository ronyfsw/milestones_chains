from modules.config import *
from modules.encoders import *
from modules.nodes import *
from modules.filters import *
from modules.worm_modules import *
import mysql.connector as mysql


def scaffold_to_chain(scaffold):
    ids_scaffolds = redisClient.hgetall('scaffolds_nodes')
    chain = scaffold.split(node_delimiter)
    while re.findall('\D{2,}', chain[0]):
        cid = chain[0]
        chain = ids_scaffolds[cid].split(node_delimiter) + [chain[-1]]
        a = 0
    chain = node_delimiter.join(chain)
    return chain

def build_chains(G, terminal_nodes, conn_params):

    # Database connection
    private_serviceIP = '172.31.15.123'
    user, password, db_name = 'rony', 'exp8546$fs', 'MCdb'
    conn = mysql.connect(**conn_params)
    cur = conn.cursor()
    cur.execute("SET SESSION TRANSACTION ISOLATION LEVEL READ COMMITTED")

    pid = os.getpid()
    print('{p} started'.format(p=pid))

    # Initialized the next journey steps
    root_node = list(nx.topological_sort(G))[0]
    root_successors = tuple(G.successors(root_node))
    redisClient.hset('scaffolds', 1, root_node)
    next_journeys_steps = [(1, root_successors)]

    growth_tip = ['no tip']
    chains_results_rows = []
    journey = chains_written_count = 0

    while next_journeys_steps:
        # Journey tracker values initiation
        journey_chains_count = overlap_count = 0
        journey_start = time.time()
        steps_chunk = next_journeys_steps[:journey_chunk]
        next_journeys_steps = next_journeys_steps[journey_chunk:]
        steps_produced, maps_produced = [], []
        journey += 1
        next_count = len(steps_chunk)
        print(60 * '*')
        print('***** journey {g}: {n} steps | pid: {p}*****'.format(g=journey, n=next_count, p=pid))

        executor = ProcessPoolExecutor(available_executors)
        if next_count <= available_executors:
            executor = ProcessPoolExecutor(next_count)

        # Build chains
        start = time.time()
        ids_chains = []
        for cid, chain, next_steps in executor.map(growReproduce, steps_chunk):
            if cid:
                ids_chains.append((cid, chain))
                steps_produced += next_steps
        grow_reproduceD = round(time.time() - start, 2)

        # Identify none-unique IDs
        start = time.time()
        db_keys = list(redisClient.hkeys('scaffolds'))
        ids_chains = checkReviseKeysOverlap(ids_chains, db_keys)
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
            cur.execute(statement)
            conn.commit()
            journey_chains_count = len(chains_results_rows)
            chains_written_count += journey_chains_count
            # print('chains_results_rows:', chains_results_rows)
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
        journeyD = round(time.time() - journey_start, 2)
        print('journey=', journeyD)

        print('{n1} next journey scaffolds | {n2} journey chains | {n3} chains'
              .format(n1=scaffolds_count, n2=journey_chains_count, n3=chains_written_count))

        # Write tracker values
        tracker_row = [journey, next_count, scaffolds_count, \
                       journey_chains_count, chains_written_count, \
                       overlap_count, grow_reproduceD, unique_idsD, \
                       write_scaffoldsD, update_mapsD, \
                       write_chainsD, next_stepsD, journeyD]
        print(tracker_row)
        statement = insert_row('{db}.{tt}'.format(db=db_name, tt=tracker_table), list(tracker_cols_types.keys()),
                               tracker_row)
        cur.execute(statement)
        conn.commit()

    # Write the remaning results
    statement = insert_rows('{db}.chains'.format(db=db_name), chains_cols, chains_results_rows)
    cur.execute(statement)
    conn.commit()
    chains_built = '{p} finished'.format(p=pid)
    return chains_built