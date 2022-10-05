from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from libraries import *

def infer_dt_format(dt):
    '''
    Infer the format of dt string to use as parameter in pd.to_datetime
    :param dt(str): A sample of a single date from the dates column for which the format is to be inferred
    '''
    dt_format = "%a %b %d %H:%M:%S %Z %Y"
    seps = ['/', '-']
    if any(sep in dt for sep in seps):
        for sep in seps:
            parts = dt.split(sep)
            if len(parts[0]) == 4: #2023-04-19
                dt_format = "%Y{}%m{}%d".format(sep, sep)
            else:
                dt_format = "%d{}%m{}%Y".format(sep, sep)
            break
    return dt_format

def activities_duration(project_df, calculate):
    '''
    Calculate the planned and actual duration for program activities
    :param project_df (DataFrame): Planned/Actual Start/End times for each activity
    :param calculate(str; planned, actual): The type of duration to calculate
    '''
    if calculate == 'planned':
        headers = ['PlannedStart', 'PlannedEnd']
    else:
        headers = ['ActualStart', 'ActualEnd']
    print('Calculating {c} duration'.format(c=calculate))
    print('{n} tasks'.format(n=len(project_df)))
    project_df = project_df[['ID'] + headers].replace('', np.nan).dropna().astype(str)
    print('{n} tasks have {c} start/end data'.format(n=len(project_df), c=calculate))
    for header in headers:
        header_sample = project_df[header].values[0]
        dt_format = infer_dt_format(header_sample)
        project_df[header] = [datetime.strptime(date_string, dt_format) for date_string in list(project_df[header])]

    project_df['Duration'] = (project_df[headers[1]] - project_df[headers[0]]).dt.days.astype(float)
    print('{n} tasks in results'.format(n=len(project_df)))
    return dict(zip(list(project_df['ID']), list(project_df['Duration'])))

def objects_encoder(objectsToEncode, use_floats=True):
	'''
	Encode strings as a floating point number
	:param strings (list): A list of strings
	:return: A dictionary of strings: flaoting point number
	'''
	encoder = {}
	if use_floats: numeric_val, increment, decimal = 0.0, 0.1, 4
	else: numeric_val, increment, decimal = 0, 1, 0
	for index, objectToEncode in enumerate(objectsToEncode):
		numeric_val = round(increment*(index+1), decimal)
		encoder[objectToEncode] = str(numeric_val)
	return encoder

def build_decoder(encoder):
	'''
	Transform an encoder to a decoder
	:param encoder: Key-Value encoding dictionary
	:return: Value-Key decoding dictionary
	'''
	return {v: k for k, v in encoder.items()}

def encode_string(text_string):
	'''
	Encode a string to produce a key
	:param text_string(str): A text string to encode
	return (str): A key for the text
	'''
	# Retrieve a numeric hash
	key = hash(text_string)
	# Add division to decimal to reduce increase hash diversity
	div = 10 ** random.sample(list(np.arange(1, 7)), 1)[0]
	key = key/div
	# Add letters to reduce increase hash diversity
	chars = list('abcdefghijklmnopqrstuvwxyz')
	random_letters = '{a}{b}{c}'.format(a=random.choice(chars), b=random.choice(chars), c=random.choice(chars))
	key = random_letters + str(key)
	return key

def tasks_rows(index_chunk):
    '''
    Transform a tasks chain into the a tasks based table (prt format)
    :param index_chunk: A chunk of the chains and the metadata files to be used in
    transforing and writing them in a prt format
    '''
    chunk_index, indices_chains, metadata_duration, tasks_types,\
    chunks_path, node_delimiter, results_cols, links_types = index_chunk
    md_ids = list(metadata_duration['ID'])
    print('{n} ids in metadata_duration'.format(n=len(md_ids)))
    results_file_path = os.path.join(chunks_path, 'results_copy_{c}.parquet'.format(c=chunk_index))
    rows = []
    for index_chain in indices_chains:
        chain_index, chain = index_chain
        chain_index = 'C{i}'.format(i=str(chain_index + 1))
        tasks = chain.split(node_delimiter)
        for index, task in enumerate(tasks):
            # Task index
            if tasks_types == 'tdas':
                task_index = 'T{i}'.format(i=str(index+1))
            else:
                task_index = 'M{i}'.format(i=str(index+1))
            task_index = chain_index+task_index
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
            rows.append((task, chain_index, task_index, next_task, pair_edge_type))
    chain_rows = []
    for row in rows:
        id = row[0]
        if id in md_ids:
            row_md = list(metadata_duration[metadata_duration['ID'] == id].values[0])[1:]
            row = list(row) + row_md
            row = tuple([str(e) for e in row])
            chain_rows.append(row)

    results_rows = pd.DataFrame(chain_rows, columns=results_cols).drop_duplicates()
    results_rows.to_parquet(results_file_path, index=False, compression='gzip')
    return len(chain_rows)
