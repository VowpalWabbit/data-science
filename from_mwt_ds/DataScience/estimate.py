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
            for key, value in config[policy].items():
                key_ = key
                value_ = value
            for estimator in config.get(policy):
                estimator_ = (config.get(policy)[estimator][0])
        return { key_ + "_" +  estimator_ : [e.get()]}


def process_input(input_files) -> tmp_df:
    pass