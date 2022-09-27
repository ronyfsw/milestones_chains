# run statement: python service.py <file_name> <experiment_name> <'tdas'> <'prt'>
import pandas as pd

print('start')
import os, sys, pathlib
modules_dir = os.path.join(pathlib.Path.home(), 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)

#from modules.worm_modules import *
from db_tables import *
from graphs import *
from directories import *
from chains import *

start_time = datetime.now().strftime("%H:%M:%S")
print('service started on', start_time)

parser = argparse.ArgumentParser()
parser.add_argument('data_file_name')
parser.add_argument('experiment')
parser.add_argument('tasks_types')
parser.add_argument('results')
args = parser.parse_args()
print('args:', args)
data_file_name = args.data_file_name
experiment = args.experiment
tasks_types = args.tasks_types
results = args.results
chains_table = '{e}_chains'.format(e=experiment)
executor = ProcessPoolExecutor(available_executors)

# Data
s3_resource.Bucket(data_bucket).download_file(data_file_name, data_file_name)
print('file {f} downloaded'.format(f=data_file_name))
G = build_graph(data_file_name)
Gnodes, Gedges = list(G.nodes()), G.edges()
# Link types dictionary
links = G.edges(data=True)
links_types = {}
for link in links: links_types[(link[0], link[1])] = link[2]['Dependency']
np.save(os.path.join(run_dir_path, 'links_types.npy'), links_types)

# Encode nodes
nodes_encoder = objects_encoder(Gnodes)
nodes_decoder = build_decoder(nodes_encoder)
np.save(os.path.join(run_dir_path, 'nodes_encoder.npy'), nodes_encoder)
np.save(os.path.join(run_dir_path, 'nodes_decoder.npy'), nodes_decoder)
G = nx.relabel_nodes(G, nodes_encoder)
Gnodes, Gedges = list(G.nodes()), G.edges()
root_node = list(nx.topological_sort(G))[0]
root_successors = list(G.successors(root_node))
isolates = graph_isolates(G)
Gnodes, Gedges = list(G.nodes()), list(G.edges())
print('Graph with {n} nodes and {e} edges'.format(n=len(Gnodes), e=len(Gedges)))
terminal_nodes = get_terminal_nodes(G)
with open(os.path.join(run_dir_path,'terminal_nodes.txt'), 'w') as f:
    f.write('\n'.join(terminal_nodes))

# Nodes types dictionary
nodes_types = {}
nodes_data = list(G.nodes(data=True))
for node_data in nodes_data:
    node_id = node_data[0]
    node_type = node_data[1]['TaskType']
    nodes_types[node_id] = node_type
np.save(os.path.join(run_dir_path, 'nodes_types.npy'), nodes_types)

# Refresh results tables and databases
redisClient.flushdb()
successorsDB.flushdb()
cur.execute("DROP TABLE IF EXISTS {db}.{t}".format(db=db_name, t=chains_table))
statement = build_create_table_statement(db_name, chains_table, chains_cols_types)
print(statement)
cur.execute(statement)

for Gnode in Gnodes:
	if Gnode not in isolates:
		successorsDB.set(Gnode, ','.join(list(G.successors(Gnode))))

## Partitions
# Sub graphs of the source program graph
run_paths = ''
chains = []
print('{n} root_successors will be used to build sub-graphs'.format(n=len(root_successors)))
for index, root_successor in enumerate(root_successors):
    nodes_to_drop = [s for s in root_successors if s != root_successor] + isolates
    subGnodes = [n for n in Gnodes if n not in nodes_to_drop]
    subG = G.subgraph(subGnodes)
    is_dag = nx.is_directed_acyclic_graph(subG)
    if is_dag:
        sub_graph_file_name = os.path.join(sub_graphs_path, 'sub_graph_{i}.edgelist'.format(i=index+1))
        nx.write_edgelist(subG, sub_graph_file_name)
        run_paths += "python3 build_chains.py {s} {e} {t} {r} & "\
            .format(s=sub_graph_file_name, e=experiment, t=tasks_types, r=results)
    else:
        print('graph {i} is not dag'.format(i=index+1))

# Run the pipeline in parallel on each of the subgraphs produced
run_paths = run_paths.rstrip(' &')
subprocess.run(run_paths, shell=True)
print('chains building started on', start_time)
print('chains building ended on', datetime.now().strftime("%H:%M:%S"))

# Return results in the tabular PRT format or as chains
# if results == 'prt':
subprocess.run("python3 build_results.py {f} {e} {t} {r}"
               .format(f=data_file_name, e=experiment, t=tasks_types, r=results), shell=True)

# Delete run directory and files
if 'run_dir' in os.listdir(working_dir):
    shutil.rmtree(run_dir_path)
