import pandas as pd

#  Way to workaround while it is not in pip
#  1. clone estimators/rlos2021_cfe, python3 setup.py install
#  2. copy estimators folder from rlos2021_cfe branch of estimators repo
# import estimators 

def estimate_bucket(bucket: pd.DataFrame, config: dict) -> dict: 
# list of estimations for every estimator from config
    # bucket : df 
    # config : {} of results
#   ..{} -> df 
    return {}

# TODO: write what fields are needed for function accurately
# 1. write specification for all requirements for inputs and outputs ie what is contract
# 2. tests for the test_estimate unittests using manual formula for validation
# 3. schema for what estimate package should return (ie definition for names of columns originated from config)

## next PR -> 1. function estimate_bucket (input_data, estimation_config) returns output
## tests
## larger goal: end to end with tests

