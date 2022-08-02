from bisect import bisect_left
from encoders import *
from modules.config import *

def binarySearchFilter(queries, target):
	'''
	Filter one list of numeric values but the elements presents in a second list of numeric values
	:param queries(list): The list to filter
	:param target(list): The values to use in filtering
	:return: A filtered version of the queries list
	source: https://stackoverflow.com/questions/30890402/how-to-check-if-an-element-from-list-a-is-not-present-in-list-b-in-python
	'''
	sorted_target = sorted(target)
	filtered = []
	for q in queries:
		i = bisect_left(sorted_target,  q)
		if i == len(sorted_target) or sorted_target[i] != q:
			filtered.append(q)
	return filtered

def lists_filter(queries, target):
	objects = target + queries
	object_is_list = False
	if type(objects[0]) == list:
		object_is_list = True
		objects = [tuple(i) for i in objects]
	encoder = objects_encoder(objects)
	decoder = build_decoder(encoder)
	encoded_target = [encoder[t] for t in target]
	encoded_queries = [encoder[t] for t in queries]
	encoding_filtered = set(encoded_queries).difference(set(encoded_target))
	#encoding_filtered = binarySearchFilter(encoded_queries, encoded_target)
	filtered = [decoder[i] for i in encoding_filtered]
	if object_is_list:
		filtered = [list(i) for i in filtered]
	return filtered

def filter_scaffolds(cids, scaffolds, saturation_successors, saturation_predecessors):
	scaffolds = [scaffold.split(node_delimiter) for scaffold in scaffolds]
	scaffolds = [scaffold[1:] for scaffold in scaffolds if len(scaffold) > 2]
	ids_scaffolds = list(zip(cids, scaffolds))
	filter_cids = []
	for cid, scaffold in ids_scaffolds:
		saturation = 0
		# Update node saturation
		for index, node in enumerate(scaffold):
			if index == 0:
				node1 = node
				node_successor = scaffold[index + 1]
				a1 = saturation_successors[node]
				saturation_successors[node] = [n for n in saturation_successors[node] if n != node_successor]
				a2 = saturation_successors[node]
				a = 0
			else:
				node2 = node
				node_predecessor = scaffold[index - 1]
				b1 = saturation_predecessors[node]
				saturation_predecessors[node] = [n for n in saturation_predecessors[node] if n != node_predecessor]
				b2 = saturation_predecessors[node]
				b = 0
			node_saturation = len(saturation_successors[node]) + len(saturation_predecessors[node])
			saturation += node_saturation
			d = 0
		# print(cid, saturation)
		if saturation == 0: filter_cids.append(cid)
	return filter_cids