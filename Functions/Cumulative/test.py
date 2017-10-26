from collections import defaultdict
from copy import deepcopy
import pymongo
import json
import time
import pandas as pd
from datetime import datetime, timedelta
from dateutil.parser import parse
import global_variable as var
import timeit
d = {}
d['FZ'] = {}
d['EK'] = {}
# 1. check cond and add through if cond
if 'LH' in d.keys():
    pass
else:
    d['LH'] = {}
d['FZ']['100'] = 1
# 2.add by increment in each time
d['FZ']['100'] = d['FZ']['100']+1
d['FZ']['110'] = 1
d['FZ']['117'] = 3
d['FZ']['250'] = 7
d['FZ']['175'] = 4
d['EK']['110'] = 1
d['EK']['117'] = 5
d['EK']['250'] = 8
d['EK']['175'] = 2
d['LH']['300'] = 2
print(d)
# 3. check next frequency
for key, value in d.iteritems():
    airline = key
    minvalue = max(value.values())
    for k in value.keys():
        if value[k] == minvalue:
            print airline+" "+k+" frequency "+str(value[k])
    #print(min(value, key=value.get))



