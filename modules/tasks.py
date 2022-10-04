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