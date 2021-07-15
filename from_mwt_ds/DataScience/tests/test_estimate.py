import unittest, pandas as pd
from estimate import estimate_bucket


class TestEstimate(unittest.TestCase):
    def  test_estimate_bucket_empty(self):
        df = pd.DataFrame
        config = {}
        self.assertEqual({}, estimate_bucket({},{}))

    def test_estimate_bucket_with_ips(self):
        df = pd.DataFrame({'p': [0.5], 'r': [1], 'policy_1': [0.5]})
        config = {  
            'policies':{'policy_1':['ips']}
        }
        self.assertEqual(
            {'policy_1_ips': 1}, estimate_bucket(df, config))

    def test_estimate_bucket_with_snips(self):
        df = pd.DataFrame({'p': [0.5], 'r': [1], 'policy_1': [0.5]})
        config = {  
            'policies':{'policy_1':['snips']}
        }
        self.assertEqual(
            {'policy_1_snips': 1}, estimate_bucket(df, config))

    def test_estimate_bucket_with_mle(self):
        df = pd.DataFrame({'p': [0.5], 'r': [1], 'policy_1': [0.5]})
        config = {  
            'policies':{'policy_1':['mle']}
        }
        self.assertEqual(
            {'policy_1_mle': 1}, estimate_bucket(df, config))

    def test_estimate_bucket_with_cressieread(self):
        df = pd.DataFrame({'p': [0.5], 'r': [1], 'policy_1': [0.5]})
        config = {  
            'policies':{'policy_1':['cressieread']}
        }
        self.assertEqual(
            {'policy_1_cressieread': 1}, estimate_bucket(df, config))
