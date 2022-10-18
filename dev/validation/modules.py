def validate(chain, source_pairs):
	'''
	Validate a single chain by the successor-predecessor relationship of its nodes
	:param chain(list): A chain to validate
	:param source_pairs(dict): The successor-predecessor nodes relationship in the source graph data
	:return: Successor-predcessor pairs identified in the chain that are not part of the source pairs
	'''

	if chain:
		pairs = []
		chain_pairs = []
		for index, id in enumerate(chain):
			if index < len(chain)-1:
				chain_pairs.append((id, chain[index+1]))
		pairs += chain_pairs
		pairs = list(set(pairs))
		pairs_in_source = [p for p in pairs if p in source_pairs]
		error_pairs = list(set(pairs).difference(set(pairs_in_source)))
	else:
		error_pairs = []
	return error_pairs