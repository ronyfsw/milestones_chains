import pandas as pd

from modules.libraries import *
from modules.parsers import *
from modules.evaluate import *
from modules.graphs import *
from modules.config import *

# Tasks metadata
file_path = os.path.join(data_path, data_file_name)
graphml_str = open(file_path).read().replace('&amp;', '')
headers = ['ID', 'TaskType', 'Label', 'PlannedStart', 'PlannedEnd', 'ActualStart', 'ActualEnd', 'Float', 'Status']
data_df = parse_graphml(data_file_name, graphml_str, headers)

# Filter TDAs
if not TDAs_in_results:
    data_df = data_df[data_df['TaskType'] != 'TT_Task']

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
fill_vals = {'PlannedStart': fill_date, 'PlannedEnd': fill_date, 'ActualStart':fill_date, 'ActualEnd': fill_date}
tasks_duration.fillna(value=fill_vals, inplace=True)
#tasks_duration[['PlannedStart', 'PlannedEnd', 'ActualStart', 'ActualEnd']] = tasks_duration[['PlannedStart', 'PlannedEnd', 'ActualStart', 'ActualEnd']].fillna()
#.apply(pd.to_datetime, errors='ignore').dt.strftime('%Y-%m-%d')
#tasks_duration['PlannedEnd'] = pd.to_datetime(tasks_duration['PlannedEnd'])
a = tasks_duration[['PlannedStart', 'PlannedEnd', 'ActualStart', 'ActualEnd']]
tasks_duration.to_excel('metadata_duration.xlsx', index=False)


