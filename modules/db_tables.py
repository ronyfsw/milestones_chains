from pathlib import Path
import os
import sys
home_dir = Path.home()
modules_dir = os.path.join(home_dir, 'services/milestones_chains/modules/')
if modules_dir not in sys.path: sys.path.append(modules_dir)
from libraries import *
from config import *

def build_create_table_statement(db_name, table_name, cols_types):

    '''
    Build Create table statement with the cluster_key and types for each column
    :param table_name(str): The name of the table to create
    :param cols_types(dict): Column names(keys) and types(values)
    '''
    cols, cols_types = list(cols_types.keys()), list(cols_types.values())
    statement_cols_types = ''
    for index, col in enumerate(cols):
        col = col.lower().replace(' ', '_').replace('%', 'perc')
        type = cols_types[index]
        col_type_str = '{c} {t},'.format(c=col, t=type)
        statement_cols_types += col_type_str
    statement_cols_types = statement_cols_types.rstrip(',')
    return "CREATE TABLE IF NOT EXISTS {db}.{tn} ({ct});".format(db=db_name, tn=table_name, ct=statement_cols_types)

# was: insert_into_table_statement
def insert_row(db_name, table_name, cols, cols_vals):

    '''
    Update table
    :param db_name(str): The name of the database to connect using the engine
    :param table_name(str): The name of the table to create
    :param cols(list): Table column cluster_key
    :param cols_vals(list): Column values
    '''
    cols_str, vals_str = "(", "("
    for index, col in enumerate(cols):
        val = cols_vals[index]
        vals_str += "'{v}',".format(v=val)
        cols_str += "{c},".format(c=col)
    cols_str = cols_str.rstrip(",")+")"
    vals_str = vals_str.rstrip(",")+")"
    return "INSERT INTO {db}.{tn} {cv} VALUES {vs}".format(db=db_name, tn=table_name, cv=cols_str, vs=vals_str)

def insert_rows(db_name, table_name, cols, cols_vals):
    '''
    Update table with multiple rows
    :param db_name(str): The name of the database to connect using the engine
    :param table_name(str): The name of the table to create
    :param cols(list): Table column cluster_key
    :param cols_vals(list of tuples): Rows values, each row as a tuple

    INSERT INTO
	projects(name, start_date, end_date)
    VALUES
	('AI for Marketing','2019-08-01','2019-12-31'),
	('ML for Sales','2019-05-15','2019-11-20');
    INSERT INTO MCdb.chains(worm,chain,nodes) VALUES ('1','0','A1170'),('1','2','MWH06.C1.CS1110')

    '''
    vals_str, cols_str = "", "("
    for col_val in cols_vals:
        vals_str += "({v}),".format(v=','.join(["'{v}'".format(v=str(v)) for v in col_val]))
    for col in cols:
        cols_str += "{c},".format(c=col)
    cols_str = cols_str.rstrip(",")+")"
    vals_str = vals_str.rstrip(",") #+")"
    statement = "INSERT INTO {db}.{tn}{cv} VALUES {vs}"\
        .format(db=db_name, tn=table_name, cv=cols_str, vs=vals_str)
    return statement
