import unittest, pandas as pd
from estimate import estimate_bucket
# df needs to have probability (p), reward (r) and probability of policy to be estimated for counterfactual

# ips = \sum(r * p_est / p) / \sum(1)
# snips = \sum(r * p_est / p) / \sum(p_est / p)

class TestEstimate(unittest.TestCase):
    # def test_estimate_bucket(self):
        # df = pd.DataFrame({'p': [0.5], 'r': [1], 'policy_1': [0.5]})
        # config = {
        #     'policies':{'policy_1':['ips','snips']}
        # }

        # self.assertEqual(
        #     {'policy_1_ips': [1], 'policy_1_snips': [1]}, estimate_bucket(df, config))

    def  test_estimate_bucket_empty(self):
        df = {}
        config = {}
        self.assertEqual({}, estimate_bucket({},{}))

    def test_estimate_bucket_only_ips(self): # need to use estimate_bucket
        df = pd.DataFrame({'p': [0.5], 'r': [1], 'policy_1': [0.5]})
        config = {
            'policies':{'policy_1':['ips']}
        }
            # I need r from df as the, and to append the list value of the policy to the key of the policy key
        self.assertEqual(
            {'policy_1_ips': [1]}, estimate_bucket(df, config)) # work in estimate.py to get this to pass
            # expects:  {'policy_1_ips': [1]}
  
    #     self.assertEqual(
    #         {'policy_1_ips': [1]}, estimate_bucket(df, config))

    # def test_estimate_bucket_more_sophisticated(self):
    #     df = pd.DataFrame({'p': [0.5, 0.8], 'r': [1, 0.4], 'policy_1': [0.5, 0]})
    #     config = {
    #         'policies':{'policy_1':['ips','snips']}
    #     }

    #     self.assertEqual(
    #         {'policy_1_ips': [1], 'policy_1_snips': [1]}, estimate_bucket(df, config))


    
