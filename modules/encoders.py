from decimal import Decimal

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

def object_encoder(objectToEncode, encoder):
	'''
	Encode a text string using an encoder
	:param objectToEncode (str): A string to encode
	:param encoder: Encoder
	:return: String-Code tuple
	'''
	objectToEncode = tuple(objectToEncode)
	if objectToEncode in encoder.keys():
		object_code = encoder[objectToEncode]
	else:
		encoder_values = list(encoder.values())
		if type(encoder_values[0]) == float: decimal = 2
		else: decimal = 0
		increment = min(encoder_values)
		max_code = max(encoder_values)
		object_code = round(max_code+increment, decimal)
	return object_code

def build_decoder(encoder):
	'''
	Transform an encoder to a decoder
	:param encoder: Key-Value encoding dictionary
	:return: Value-Key decoding dictionary
	'''
	return {v: k for k, v in encoder.items()}

def decode_chains(chains, mapper, nodes_delimiter='<>'):
    '''
    Decode chains produced in a pipeline with an encoder
    :param chains(list): The chains to decode (each chain is given as a string of nodes delimited by the nodes delimiter)
    :param mapper(dict): A mapping of the chain code to the chain ID
    :return:
    '''
    decoded_chains = []
    for chain in chains:
        chain = [mapper[str(n)] for n in chain.split(nodes_delimiter)]
        decoded_chains.append(nodes_delimiter.join(chain))
    return decoded_chains


def decimal_representation(num):
    '''
	Represent a number as a decimal
	:param num (integer): The number to represent
	:return: A decimal representation of the input number
	For example: 12345 will be return 0.12345
	'''
    num_digits = len(str(num))
    return num / (10 ** num_digits)


def decimal_concatenation(num1, num2):
    '''
	Build a decimal representation for a pair of number by adding the decimal representation of the second to the first
	:param (int) num1: The first number
	:param num2 (int): The second number which will be transformed to a digital representation
	:return: A decmial representation of the numbers pair, for example, 145, 47632 will give 145.47632
	'''
    return num1 + decimal_representation(num2)


def decimal_to_integer(float_num):
    '''
	Extract decimals as integer
	:param float_num (float): The number with the decimal number to transform
	:return: The transformed decimal, 1.23 -> 23
	'''
    float_num = Decimal(str(float_num))
    dec_only = Decimal(float_num - int(float_num))
    dec_count = Decimal(len(str(dec_only))-2)
    return int(dec_only * (10 ** dec_count))


def assert_unique_key(dict_keys, key):
	'''
	Assert that a key is not in a dictionary and if it is, build use the key to build a new one
	:param dict_keys(string or object name): The dictionary keys to explore.
	:param key (float): A numeric key
	:return: The input key if it is unique or a new key if the key is already in the dictionary
	'''
	dict_keys = list(dict_keys)
	while str(key) in dict_keys:
		#print(key, 'in')
		key += 1
		dict_keys.append(key)
	return key


