import pandas as pd
import estimators
from estimate import estimators
# these imports ie bandits and snips aren't working for me
# from estimators import bandits
# from bandits import snips

def estimate_bucket(bucket: pd.DataFrame, config: dict) -> dict: 
    # init estimaors
    ipssnips = snips.Estimator()
    if len(bucket) == 0 or config == {}:
        return {}
    else:
        pass