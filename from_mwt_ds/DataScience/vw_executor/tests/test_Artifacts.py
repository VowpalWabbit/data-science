import unittest
from vw_executor.artifacts import *

class TestOutput(unittest.TestCase):
    def test_output_cb(self):
        output = Output('vw_executor/tests/data/artifacts/stdout_cb.txt')
        self.assertEqual(output.loss, -0.88)
        self.assertEqual(len(output.loss_table), 10)

class TestPredictions(unittest.TestCase):
    def test_predictions_scalar(self):
        predictions = Predictions('vw_executor/tests/data/artifacts/pred_scalar.txt')
        self.assertEqual(len(predictions.scalar), 21)   

    def test_predictions_cb(self):
        predictions = Predictions('vw_executor/tests/data/artifacts/pred_cb.txt')
        self.assertEqual(len(predictions.cb), 10)  

    def test_predictions_ccb(self):
        predictions = Predictions('vw_executor/tests/data/artifacts/pred_ccb.txt')
        self.assertEqual(len(predictions.ccb), 22)        