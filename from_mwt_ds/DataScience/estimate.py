import pandas as pd
from estimators import bandits
from estimators.bandits import ips

def estimate_bucket(bucket: pd.DataFrame, config: dict) -> dict:
    if len(bucket) == 0 or config == {}:
        return {}
    else:
        e = bandits.ips.Estimator()
        for row in bucket:
            bucket.get(row)
        for policy in config:
            config.get(policy)
            for estimator in config.get(policy):
                config.get(policy)[estimator]