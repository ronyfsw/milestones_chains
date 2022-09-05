from modules.config import *
def scaffold_to_chain(scaffold):
    ids_scaffolds = redisClient.hgetall('scaffolds_nodes')
    chain = scaffold.split(node_delimiter)
    while re.findall('\D{2,}', chain[0]):
        cid = chain[0]
        chain = ids_scaffolds[cid].split(node_delimiter) + [chain[-1]]
        a = 0
    chain = node_delimiter.join(chain)
    return chain

def chain_to_rows(index_chain, links_types):
	rows = []
	chain_index, tasks = index_chain
	for index, task in enumerate(tasks):
		if index <= (len(tasks) - 2):
			next_task = tasks[index + 1]
		else:
			next_task = None
		if next_task:
			try:
				pair_edge_type = links_types[(task, next_task)]
			except KeyError:
				pair_edge_type = None
		else:
			pair_edge_type = None
		rows.append((task, chain_index, next_task, pair_edge_type))

	return rows

