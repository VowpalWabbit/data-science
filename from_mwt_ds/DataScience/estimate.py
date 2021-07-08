import pandas as pd
from estimators.bandits import ips, snips, mle, cressieread


def process_input(input_file) -> pd.DataFrame:
    tmp_df = pd.read_csv(input_file)
    return tmp_df

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
        tmp_df = process_input(file)
    # TODO: chunk-size
        estimate_bucket(tmp_df, config)
    return

input_files = ["test_data/cb/01.csv"]
config = {
    'policies': {'random': ['ips', 'snips'], 'baseline1': ['ips']},
    'aggregation': {'num_of_events': 10}
}
estimate(input_files, config)