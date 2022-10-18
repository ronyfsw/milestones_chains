# Filter chains for tdas
print('Filter chains for tdas')
if results != 'tdas':
    chains_rows = []
    chains_df = pd.read_sql('SELECT * FROM MCdb.{ct}'.format(ct=chains_table), con=conn)
    ids_chains = list(zip(chains_df['id'], chains_df['chain']))
    chains_nodes_types = []
    for id, chain in ids_chains:
        chain = chain.split(node_delimiter)
        chain_nodes_types = {k: v for k, v in nodes_types.items() if k in chain}
        chains_nodes_types.append((id, chain, chain_nodes_types))
    ids_chains = []
    for cid, milestones_chain in executor.map(filter_tdas, chains_nodes_types):
        chains_rows.append((cid, milestones_chain))

    print('milestone chains sample')
    print(chains_rows[:10])
    print('write milestone chains')
    cur.execute("DROP TABLE IF EXISTS {db}.{t}".format(db=db_name, t=chains_table))
    statement = build_create_table_statement(db_name, chains_table, chains_cols_types)
    print(statement)
    cur.execute(statement)
    statement = insert_rows(db_name, chains_table, chains_cols, chains_rows)
    cur.execute(statement)
    conn.commit()



