from difflib import SequenceMatcher
def pair_overlap(pair):
	overlap = False
	str1, str2 = pair
	shorter = min(pair, key=len)
	# initialize SequenceMatcher object with
	# input string
	seqMatch = SequenceMatcher(None, str1, str2)

	# find match of longest sub-string
	# output will be like Match(a=0, b=0, size=5)
	match = seqMatch.find_longest_match(0, len(str1), 0, len(str2))
	match_str = str1[match.a: match.a + match.size]
	if match_str == shorter:
		overlap = True
	return overlap, shorter