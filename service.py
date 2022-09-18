# run statement: python service.py <file_name> <experiment_name> <'tdas'> <'prt'>
print('start')
from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)

#from modules.worm_modules import *
from db_tables import *
from graphs import *
from worm_modules import *

start_time = datetime.now().strftime("%H:%M:%S")
print('pipeline started on', start_time)

parser = argparse.ArgumentParser()
parser.add_argument('data_file_name')
parser.add_argument('experiment')
parser.add_argument('tasks_types')
parser.add_argument('prt')
args = parser.parse_args()
print('args:', args)
data_file_name = args.data_file_name
experiment = args.experiment
tasks_types = args.tasks_types
prt = args.prt
chains_table = '{e}_chains'.format(e=experiment)
TDAs_in_results = build_rows = False
if tasks_types == 'tdas': TDAs_in_results = True
if build_rows == 'prt': build_rows = True

# Data
s3_resource.Bucket(data_bucket).download_file(data_file_name, data_file_name)
print('file {f} downloaded'.format(f=data_file_name))
G = build_graph(data_file_name)
Gnodes, Gedges = list(G.nodes()), G.edges()
terminal_nodes = get_terminal_nodes(G)

# Encode nodes
nodes_encoder = objects_encoder(Gnodes)
nodes_decoder = build_decoder(nodes_encoder)
np.save('nodes_encoder.npy', nodes_encoder)
np.save('nodes_decoder.npy', nodes_decoder)
G = nx.relabel_nodes(G, nodes_encoder)
data_file_name = os.path.join(sub_graphs_path, 'graph.edgelist')
nx.write_edgelist(G, data_file_name)
Gnodes, Gedges = list(G.nodes()), G.edges()
root_node = list(nx.topological_sort(G))[0]
root_successors = list(G.successors(root_node))
isolates = graph_isolates(G)
Gnodes, Gedges = list(G.nodes()), list(G.edges())
print('Graph with {n} nodes and {e} edges'.format(n=len(Gnodes), e=len(Gedges)))
terminal_nodes = get_terminal_nodes(G)
with open('terminal_nodes.txt', 'w') as f: f.write('\n'.join(terminal_nodes))

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
        sub_graph_file_name = os.path.join(sub_graphs_path, 'sub_graph_{i}.edgelist'.format(i=index+1))
        nx.write_edgelist(subG, sub_graph_file_name)
        run_paths += "python3 build_chains.py {s} {e} & "\
            .format(s=sub_graph_file_name, e=experiment)
    else:
        print('graph {i} is not dag'.format(i=index+1))

# Run the pipeline in parallel on each of the subgraphs produced
run_paths = run_paths.rstrip(' &')
print('run_paths:', run_paths)
subprocess.run(run_paths, shell=True)
print('pipelines started on', start_time)
print('pipelines ended on', datetime.now().strftime("%H:%M:%S"))

if build_rows:
    subprocess.run("python3 build_rows.py {e} {f} {t}"
                   .format(e=experiment, f=data_file_name, t=tasks_types), shell=True)
