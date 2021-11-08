import unittest
from vw_executor.vw import *

class TestVw(unittest.TestCase):
    input1 = 'vw_executor/tests/data/cb_0.json'
    input2 = 'vw_executor/tests/data/cb_1.json'

    def test_data_shape(self):
        vw = Vw('.vw_cache', handlers = [])
        result = vw.train(self.input1, '--cb_explore_adf --dsjson')
        self.assertEqual(isinstance(result, Job), True)
        self.assertEqual(len(result), 1)