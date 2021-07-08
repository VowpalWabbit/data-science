import pandas as pd
from estimators.bandits import ips, snips, mle, cressieread


def process_input(input_file, chunksize) -> pd.DataFrame:
    for chunk in pd.read_csv(input_file, chunksize=chunksize):
        return chunk

def estimate_bucket(bucket: pd.DataFrame, config: dict) -> dict:
    if len(bucket) == 0 or config == {}:
        return {}
    else:
        for policy_name, estimator_list in config["policies"].items():
            estimator_obj = None
            for estimator in estimator_list:
                if estimator == 'ips':
                    estimator_obj = ips.Estimator()
                elif estimator == 'snips':
                    estimator_obj = snips.Estimator()
                elif estimator == 'mle':
                    estimator_obj = mle.Estimator()
                elif estimator == 'cressieread':
                    estimator_obj = cressieread.Estimator()
                else:
                    raise("Estimator not found.")

                for index, row in bucket.iterrows():
                    # TODO: policy name is binary
                    estimator_obj.add_example(row['p'], row['r'], row[policy_name])
                # for policy in config:
                #     for key, value in config[policy].items():
                #         key_ = key
                #         value_ = value
                #     for estimator in config.get(policy):
                #         estimator_ = (config.get(policy)[estimator][0])
        return {}

def estimate(input_files, config):
    number_of_events = config["aggregation"]['num_of_events']
    for file in input_files:
        chunksize = number_of_events
        tmp_df = process_input(file, chunksize)
        estimate_bucket(tmp_df, config)
    return 

input_files = ["test_data/cb/01.csv"]
config = {
    'policies': {'random': ['ips', 'snips'], 'baseline1': ['ips']},
    'aggregation': {'num_of_events': 10}
}
estimate(input_files, config)