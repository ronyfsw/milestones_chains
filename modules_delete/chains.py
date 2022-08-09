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
