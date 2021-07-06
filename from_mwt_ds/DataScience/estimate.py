import pandas as pd
from estimators import bandits
from estimators.bandits import ips

def estimate_bucket(bucket: pd.DataFrame, config: dict) -> dict:
    if len(bucket) == 0 or config == {}:
        return {}
    else:
        e = bandits.ips.Estimator()
        for index, row in df.iterrows():
            e.add_example(row['p'], row['r'], row['policy_1'])
        for policy in config:
            config.get(policy).keys()
            for estimator in config.get(policy):
                print(config.get(policy)[estimator])
        return e.get()