def drop_chain_overlaps(chains):
    chains = list(set([c for c in chains if c]))
    chains.sort(key=len)
    exclude, keep = [], []
    while chains:
        chain1 = chains[0]
        chains.remove(chain1)
        for chain2 in chains:
            if chain1 in chain2:
                exclude.append(chain1)
                break
        if chain1 not in exclude: keep.append(chain1)
    return keep