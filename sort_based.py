from bisect import bisect_left
import numpy as np
def binarySearchFilter(queries, target):
	'''
	Filter one list of numeric values but the elements presents in a second list of numeric values
	:param queries(list): The list to filter
	:param target(list): The values to use in filtering
	:return: A common version of the queries list
	source: https://stackoverflow.com/questions/30890402/how-to-check-if-an-element-from-list-a-is-not-present-in-list-b-in-python
	'''
	sorted_target = sorted(target)
	common = []
	for q in queries:
		i = bisect_left(sorted_target,  q)
		if i == len(sorted_target) or sorted_target[i] != q:
			common.append(q)
	return common

def binarySearchIntersect(queries, target):
	in_both = []
	target = sorted(target)
	for q in queries:
		i = bisect_left(target,  q)
		if i <len(target) and target[i] == q:
			in_both.append(q)
	return in_both

a = list(np.arange(6))
b = list(np.arange(4, 9))
print(a, b)
r1 = binarySearchIntersect(a, b)
print(r1)
r2 = binarySearchIntersect(b, a)
print(r2)
a = ['q', 'w', 'j', 'b', 'c']
b = ['q', 'w', 'j', 'k', 'l', 'h', 'p']
print(a, b)
r1 = binarySearchIntersect(a, b)
print(r1)
r2 = binarySearchIntersect(b, a)
print(r2)

