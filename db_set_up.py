from modules.config import *
from modules.libraries import *
from modules.graphs import *
from modules.chains import *
from modules.encoders import *
from modules.nodes import *
from modules.filters import *
from modules.worm_modules import *

start_time = datetime.now().strftime("%H:%M:%S")
print('pipeline started on', start_time)

# Refresh results tables and databases
redisClient.flushdb()
successorsDB.flushdb()
predecessorsDB.flushdb()
cur.execute("DROP TABLE IF EXISTS {db}.{t}".format(db=db_name, t=chains_table))
statement = build_create_table_statement(db_name, chains_table, chains_cols_types)
print(statement)
cur.execute(statement)
cur.execute("DROP TABLE IF EXISTS {db}.{t}".format(db=db_name, t=tracker_table))
statement = build_create_table_statement(db_name, tracker_table, tracker_cols_types)
cur.execute(statement)
print(statement)
