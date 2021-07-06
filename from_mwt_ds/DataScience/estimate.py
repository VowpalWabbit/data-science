import pandas as pd
from estimators import bandits
from estimators.bandits import ips

def estimate_bucket(bucket: pd.DataFrame, config: dict) -> dict:
    if len(bucket) == 0 or config == {}:
        return {}
    else:
        e = bandits.ips.Estimator()
        for index, row in bucket.iterrows():
            e.add_example(row['p'], row['r'], row['policy_1'])
        for policy in config:
            config.get(policy).keys() # prints {'policy_1' : ['ips']}
            for estimator in config.get(policy):
                estimator_ = (config.get(policy)[estimator][0])
        return config.get(policy).keys(), estimator_, [e.get()]
 