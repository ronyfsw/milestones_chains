import psycopg2
from psycopg2.extensions import ISOLATION_LEVEL_AUTOCOMMIT
from modules.py_postgres import *
from modules.config import *
# Database
db_name = 'mcdb1'
# conn = psycopg2.connect(database="postgres", user='rony', password='1234', host='localhost', port='5432')
# conn.set_isolation_level(ISOLATION_LEVEL_AUTOCOMMIT)
# create_db(db_name, conn)

# Table
conn = psycopg2.connect(database=db_name, user='rony', password='1234', host='localhost', port='5432')
data_types = list(results_cols_types.values())
create_table(db_name, results_table, results_cols, data_types, conn)
