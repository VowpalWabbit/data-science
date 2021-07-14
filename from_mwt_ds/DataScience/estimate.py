import pandas as pd
from estimators.bandits import ips, snips, mle, cressieread

def estimate_bucket(bucket: pd.DataFrame, config: dict) -> dict:
    result = {}
    if len(bucket) == 0 or config == {}:
        return result
    for policy_name, estimator_list in config["policies"].items():
        estimator = None
        for estimator_name in estimator_list:
            if estimator_name == 'ips':
                estimator = ips.Estimator()
            elif estimator_name == 'snips':
                estimator = snips.Estimator()
            elif estimator_name == 'mle':
                estimator = mle.Estimator()
            elif estimator_name == 'cressieread':
                estimator = cressieread.Estimator()
            else:
                raise("Estimator not found.")

            for _, row in bucket.iterrows():
                estimator.add_example(row['p'], row['r'], row[policy_name])
            estimated_reward = estimator.get()
            result[policy_name + "_" + estimator_name] = estimated_reward   
    return result

def estimate(input_files, config):
    number_of_events = config["aggregation"]['num_of_events']
    for file in input_files:
        for chunk in pd.read_csv(file, chunksize=number_of_events):
            estimate_bucket(chunk, config)
    return
