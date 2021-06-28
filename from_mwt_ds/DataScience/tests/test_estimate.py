import unittest, pandas as pd
from estimate import estimate_bucket, estimate_bucket_only_ips
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

    def test_estimate_bucket_only_ips(self):
        df = pd.DataFrame({'p': [0.5], 'r': [1], 'policy_1': [0.5]})
        config = {
            'policies':{'policy_1':['ips']}
        }

        self.assertEqual(
            {'policy_1_ips': [1]}, estimate_bucket_only_ips(df, config))
  
    #     self.assertEqual(
    #         {'policy_1_ips': [1]}, estimate_bucket(df, config))

    # def test_estimate_bucket_more_sophisticated(self):
    #     df = pd.DataFrame({'p': [0.5, 0.8], 'r': [1, 0.4], 'policy_1': [0.5, 0]})
    #     config = {
    #         'policies':{'policy_1':['ips','snips']}
    #     }

    #     self.assertEqual(
    #         {'policy_1_ips': [1], 'policy_1_snips': [1]}, estimate_bucket(df, config))


    
