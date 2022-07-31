import sys
import matplotlib.pyplot as plt
path = '/home/rony/Projects_Code/Milestones_Duration/modules'
if path not in sys.path: sys.path.append(path)
from modules.config import *
from modules.libraries import *
tracker_table = 'tracker_redis'
df = pd.read_sql('SELECT * FROM {t}'.format(t=tracker_table), con=conn)
print(df.head(30))
df.to_excel(os.path.join(experiment_path, 'tracker.xlsx'), index=False)
chain_vals = list(df['chains'])
step_duration_vals = list(df['stepd'])
update_duration_vals = list(df['updated'])
print('run duration for {n} chains='.format(n=chain_vals[-1]), sum(step_duration_vals))
dur0, dur1 = len(df[df['stepd'] == 0]), len(df[df['stepd'] == 1])

plt.scatter(chain_vals, step_duration_vals, marker='.', s=1)
plt.xlabel('Chains Produced')
plt.ylabel('Step Duration')
plt.savefig(os.path.join(experiment_path, 'step_duration_plot.png'))
plt.show()