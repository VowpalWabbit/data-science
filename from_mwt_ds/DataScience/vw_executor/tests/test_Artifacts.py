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
        self.assertEqual(len(list(predictions.scalar)), 21)   

    def test_predictions_cb(self):
        predictions = Predictions('vw_executor/tests/data/artifacts/pred_cb.txt')
        self.assertEqual(len(list(predictions.cb)), 11)  

    def test_predictions_ccb(self):
        predictions = Predictions('vw_executor/tests/data/artifacts/pred_ccb.txt')
        self.assertEqual(len(list(predictions.ccb)), 23)

    def test_predictions_cats(self):
        predictions = Predictions('vw_executor/tests/data/artifacts/pred_cats.txt')
        self.assertEqual(len(list(predictions.cats)), 10)

    def test_predictions_csoaa_ldf(self):
        predictions = Predictions('vw_executor/tests/data/artifacts/pred_csoaa_ldf.txt')
        self.assertEqual(len(list(predictions.csoaa_ldf)), 3)


class TestModel(unittest.TestCase):
    def test_readable_model_8(self):
        model = Model8('vw_executor/tests/data/artifacts/readable_model_8.txt')
        self.assertEqual(len(model.weights), 3)

    def test_invert_hash_8(self):
        model = Model8('vw_executor/tests/data/artifacts/invert_hash_8.txt')
        self.assertEqual(len(model.weights), 3)

    def test_readable_model_9(self):
        model = Model9('vw_executor/tests/data/artifacts/readable_model_9.txt')
        self.assertEqual(len(model.weights), 3)

    def test_invert_hash_9(self):
        model = Model9('vw_executor/tests/data/artifacts/invert_hash_9.txt')
        self.assertEqual(len(model.weights), 3)

    def test_readable_model_with_online_state_json(self):
        model = Model('vw_executor/tests/data/artifacts/readable_model_online_state.json')
        self.assertEqual(len(model.weights), 3)

    def test_invert_hash_with_online_state_json(self):
        model = Model('vw_executor/tests/data/artifacts/invert_hash_online_state.json')
        self.assertEqual(len(model.weights), 3)

    def test_readable_model_without_online_state_json(self):
        model = Model('vw_executor/tests/data/artifacts/readable_model_no_online_state.json')
        self.assertEqual(len(model.weights), 3)

    def test_invert_hash_without_online_state_json(self):
        model = Model('vw_executor/tests/data/artifacts/invert_hash_no_online_state.json')
        self.assertEqual(len(model.weights), 3)
