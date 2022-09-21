import pandas as pd

from modules.libraries import *
from modules.parsers import *
from modules.evaluate import *
from modules.graphs import *
from modules.config import *

parser = argparse.ArgumentParser()
parser.add_argument('data_file_name')
parser.add_argument('tasks_types')
args = parser.parse_args()
data_file_name = args.data_file_name
tasks_types = args.tasks_types
TDAs_in_results = False

# Tasks metadata
graphml_str = open(data_file_name).read().replace('&amp;', '')
headers = ['ID', 'TaskType', 'Label', 'PlannedStart', 'PlannedEnd', 'ActualStart', 'ActualEnd', 'Float', 'Status']
data_df = parse_graphml(data_file_name, graphml_str, headers)

print('tasks type in md prep:', tasks_types)
print('{n} tasks in md prep prior to filtering'.format(n=len(data_df)))

# Filter TDAs
if tasks_types != 'tdas':
    data_df = data_df[data_df['TaskType'] != 'TT_Task']
print('{n} tasks in md prep following filtering'.format(n=len(data_df)))

# Tasks duration
planned_duration = activities_duration(data_df, 'planned')
planned_duration_df = pd.DataFrame(list(zip(list(planned_duration.keys()), list(planned_duration.values()))), columns=['ID', 'planned_duration'])
actual_duration = activities_duration(data_df, 'actual')
actual_duration_df = pd.DataFrame(list(zip(list(actual_duration.keys()), list(actual_duration.values()))), columns=['ID', 'actual_duration'])
planned_actual_df = pd.merge(planned_duration_df, actual_duration_df, how='left')
tasks_duration = pd.merge(data_df, planned_actual_df)
dt_cols = ['PlannedStart', 'PlannedEnd', 'ActualStart', 'ActualEnd']
for col in dt_cols:
    tasks_duration[col] = pd.to_datetime(tasks_duration[col], errors='ignore').dt.strftime('%Y-%m-%d')
a = tasks_duration[['PlannedStart', 'PlannedEnd', 'ActualStart', 'ActualEnd']]
tasks_duration.to_excel('metadata_duration.xlsx', index=False)


