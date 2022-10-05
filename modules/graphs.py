from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from libraries import *

def build_graph(file_path):
    '''
    Build a directed graph of a .graphml file
    '''
    G = nx.read_graphml(file_path)
    G = nx.DiGraph(G)
    return G

def graph_isolates(G):
    '''
    Identify isolate nodes (degree = 0) in an input graph
    '''
    nodes_degrees = dict(G.degree())
    return [n for n in list(nodes_degrees.keys()) if nodes_degrees[n] == 0]

def get_terminal_nodes(G):
    '''
    Build a list of the terminal nodes (no successors) of an input graph
    '''
    Gnodes = list(G.nodes())
    isolates = graph_isolates(G)
    return [n for n in Gnodes if ((G.out_degree(n) == 0) & (n not in isolates))]

def parse_graphml(file_name, graphml_str, headers):
    '''
    Parse a graphml file contents to a dataframe
    :param graphml_str (string): The file contents to parse
    :param headers (list): The columns to parse
    '''
    # Remove umlauts
    graphml_str = graphml_str.replace('�', '')
    nodes = graphml_str.split('</node>')
    nodes = [s for s in nodes if 'node id' in s]
    nodes = [n.lstrip().rstrip() for n in nodes]
    nodes = [n.replace('"', '') for n in nodes]
    # Exclude file header
    # nodes = nodes[1:]
    nodes_df = pd.DataFrame()
    print('parsing {n} nodes'.format(n=len(nodes)))
    for index, node in enumerate(nodes):
        node_rows = node.split('\n')
        node_rows = [r for r in node_rows if re.findall('<node id|<data key', r)]
        id = re.findall('=(.*?)>', node_rows[0])[0]
        node_rows = node_rows[1:]
        keys = ['ID'] + [re.findall('=(.*?)>', n)[0] for n in node_rows]
        values = [id] + [re.findall('>(.*?)<', n)[0] for n in node_rows]
        node_df = pd.DataFrame([values], columns = keys)
        nodes_df = pd.concat([nodes_df, node_df])
    nodes_df = nodes_df[headers]
    nodes_df['File'] = file_name
    return nodes_df
