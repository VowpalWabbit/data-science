import pandas as pd
import estimators
from bandits import ips

def estimate_bucket(bucket: pd.DataFrame, config: dict) -> dict: 
    if len(bucket) == 0 or config == {}:
        return {}
    else:
        pass