import unittest
from vw_executor.artifacts import *

class TestOutput(unittest.TestCase):
    def test_output_cb(self):
        output = Output('vw_executor/tests/data/artifacts/stdout_cb.txt')
        self.assertEqual(output.loss, -0.88)
        self.assertEqual(len(output.loss_table), 10)