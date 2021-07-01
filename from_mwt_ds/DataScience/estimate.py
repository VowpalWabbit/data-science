import pandas as pd

#  Way to workaround while it is not in pip
#  1. clone estimators/rlos2021_cfe, python3 setup.py install
#  2. copy estimators folder from rlos2021_cfe branch of estimators repo
# import estimators 

def estimate_bucket(bucket: pd.DataFrame, config: dict) -> dict: 
    if len(bucket) == 0 or config == {}:
        return {}
    else:
        return_dict = {}
        for policies in config:
            firstpart = list(config.get(policies).keys()[0])
            for i in config.get(policies):
                secondpart = list(config.get(policies).values())[0][0]
        keyresult = firstpart + "_" + secondpart
        formula_ips = (df["r"][0] * df["policy_1"][0] / df["p"][0]) / 1
        return_dict[keyresult] = [formula_ips]


# list of estimations for every estimator from config
    # bucket : df 
    # config : {} of results
#   ..{} -> df 

        # for policy in config['policies']:
        #     print(policy)
        # I need r from df as the, and to append the list value of the policy to the key of the policy key
    return  return_dict # only work in this function!!!!!

# def estimate_bucket_only_ips(bucket: pd.DataFrame, config: dict) -> dict: 
#     df = pd.DataFrame({'p': [0.5], 'r': [1], 'policy_1': [0.5]})
#     config = {
#             'policies':{'policy_1':['ips']}
#     }
#     return {'policy_1_ips': [1]}



# TODO: write what fields are needed for function accurately
# 1. write specification for all requirements for inputs and outputs ie what is contract
# 2. tests for the test_estimate unittests using manual formula for validation
# 3. schema for what estimate package should return (ie definition for names of columns originated from config)

## next PR -> 1. function estimate_bucket (input_data, estimation_config) returns output
## tests
## larger goal: end to end with tests

