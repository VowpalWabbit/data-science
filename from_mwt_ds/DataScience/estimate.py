#!/usr/bin/env python
# coding: utf-8

# # Contextual Bandits data

# ## Load data

# In[4]:


import pandas as pd

df = pd.read_csv(r'test_data/cb/01.csv', parse_dates=['t']).set_index('t')
df.head()

df.tail(50)


# ## Apply estimators

from cb.estimators import ips_snips


def init_ips_snips(r, p, p_log, n):
    result = ips_snips()
    result.add(r, p_log, p, n * int(p > 0))
    return result

policies = ['random', 'baseline1']
for p in policies:
    df[p] = df.apply(lambda r: init_ips_snips(r['r'], r[f"('b', '{p}')"], r['p'], r['n']), axis = 1)

df = df[policies].resample('5min').sum()
df


import matplotlib.pyplot as plt

df.apply(lambda r: r['random'].get('snips'), axis=1).plot(label='random')
df.apply(lambda r: r['baseline1'].get('snips'), axis=1).plot(label='baseline1')

plt.legend(loc='best')


# ## Reaggregate (if needed)


df = df.resample('10min').sum()
df

