import pandas as pd
from estimators import bandits
from estimators.bandits import ips

def estimate_bucket(bucket: pd.DataFrame, config: dict) -> dict:
    if len(bucket) == 0 or config == {}:
        return {}
    else:
        e = bandits.ips.Estimator()
        e.add_example(0.5, 1.0, 0.5)
        return e.get() # 1