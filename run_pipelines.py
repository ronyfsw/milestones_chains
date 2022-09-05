print('start')
from modules.worm_modules import *
start_time = datetime.now().strftime("%H:%M:%S")
print('pipeline started on', start_time)

print('Generate tasks metadata')
subprocess.run("python3 metadata_duration.py", shell=True)
print('Generate tasks metadata completed')

# Refresh results tables and databases
redisClient.flushdb()
successorsDB.flushdb()

cur.execute("DROP TABLE IF EXISTS {db}.{t}".format(db=db_name, t=chains_table))
results_cur.execute("DROP TABLE IF EXISTS {db}.{t}".format(db=db_name, t=results_table))
# statement = build_create_table_statement(db_name, chains_table, chains_cols_types)
#statement = build_create_table_statement(db_name, chains_table, chains_cols_types)
#cur.execute(statement)

# Data
G = build_graph(file_path)
Gnodes, Gedges = list(G.nodes()), G.edges()
terminal_nodes = get_terminal_nodes(G)

# Encode nodes
nodes_encoder = objects_encoder(Gnodes)
nodes_decoder = build_decoder(nodes_encoder)
np.save('nodes_encoder.npy', nodes_encoder)
np.save('nodes_decoder.npy', nodes_decoder)
G = nx.relabel_nodes(G, nodes_encoder)
graph_path = os.path.join(sub_graphs_path, 'graph.edgelist')
nx.write_edgelist(G, graph_path)
Gnodes, Gedges = list(G.nodes()), G.edges()
root_node = list(nx.topological_sort(G))[0]
root_successors = list(G.successors(root_node))
isolates = graph_isolates(G)
Gnodes, Gedges = list(G.nodes()), list(G.edges())
print('Graph with {n} nodes and {e} edges'.format(n=len(Gnodes), e=len(Gedges)))
terminal_nodes = get_terminal_nodes(G)
with open('terminal_nodes.txt', 'w') as f: f.write('\n'.join(terminal_nodes))

for Gnode in Gnodes:
	if Gnode not in isolates:
		successorsDB.set(Gnode, ','.join(list(G.successors(Gnode))))

# Partitions
# Sub graphs of the source program graph
run_paths = ''
chains = []
for index, root_successor in enumerate(root_successors):
    nodes_to_drop = [s for s in root_successors if s != root_successor] + isolates
    subGnodes = [n for n in Gnodes if n not in nodes_to_drop]
    subG = G.subgraph(subGnodes)
    is_dag = nx.is_directed_acyclic_graph(subG)
    if is_dag:
        #subG = nx.DiGraph(subG)
        print(50*'#')
        print(root_successor, subG)
        graph_path = os.path.join(sub_graphs_path, 'sub_graph_{i}.edgelist'.format(i=index+1))
        nx.write_edgelist(subG, graph_path)
        run_paths += "python3 pipeline.py {gp} & ".format(gp=graph_path)
    else:
        print('graph {i} is not dag'.format(i=index+1))

# Run the pipeline in parallel on each of the subgraphs produced
run_paths = run_paths.rstrip(' &')
print('run_paths:', run_paths)
subprocess.run(run_paths, shell=True)

#with open('chains.txt', 'w') as f: f.write('\n'.join(chains))

print('pipelines started on', start_time)
print('pipelines ended on', datetime.now().strftime("%H:%M:%S"))