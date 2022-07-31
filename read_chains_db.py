import redis
import numpy as np
from collections import OrderedDict
from modules.config import *
from modules.encoders import *

rkeys = redisClient.hkeys('built_chains')
rkeys_vals = redisClient.hgetall('build_chains')
for k, v in rkeys_vals.items():
    print(k, v)
print(len(rkeys_vals))