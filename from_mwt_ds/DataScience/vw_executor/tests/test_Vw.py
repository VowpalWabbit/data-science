import unittest
from vw_executor.vw import *
from vw_executor.vw_opts import Grid
import pandas as pd

class TestVw(unittest.TestCase):
    input1 = 'vw_executor/tests/data/cb_0.json'
    input2 = 'vw_executor/tests/data/cb_1.json'

    def test_data_shape(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.train(self.input1, '--cb_explore_adf --dsjson')
        self.assertEqual(isinstance(result, Job), True)
        self.assertEqual(len(result), 1)

        result = vw.train(self.input1, {'#problem': '--cb_explore_adf', '#format':  '--dsjson'})
        self.assertEqual(isinstance(result, Job), True)
        self.assertEqual(len(result), 1)

        result = vw.train(self.input1, [
            '--cb_explore_adf --dsjson --epsilon 0.1',
            '--cb_explore_adf --epsilon 0.2'])
        self.assertEqual(isinstance(result, list), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(isinstance(result[0], Job), True)       
        self.assertEqual(isinstance(result[1], Job), True) 
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1)

        result = vw.train(self.input1, [
            '--cb_explore_adf --dsjson --epsilon 0.1',
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])
        self.assertEqual(isinstance(result, list), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(isinstance(result[0], Job), True)       
        self.assertEqual(isinstance(result[1], Job), True) 
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1)

        result = vw.train(self.input1, [
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.1},
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])
        self.assertEqual(isinstance(result, list), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(isinstance(result[0], Job), True)       
        self.assertEqual(isinstance(result[1], Job), True) 
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1)

        result = vw.train(self.input1, Grid({
            '#base': ['--cb_explore_adf --dsjson'],
            '--epsilon': [0.1, 0.2]
        }))
        self.assertEqual(isinstance(result, list), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(isinstance(result[0], Job), True)       
        self.assertEqual(isinstance(result[1], Job), True) 
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1)

        result = vw.train([self.input1, self.input2], '--cb_explore_adf --dsjson')
        self.assertEqual(isinstance(result, Job), True)
        self.assertEqual(len(result), 2)

        result = vw.train([self.input1, self.input2], {'#problem': '--cb_explore_adf', '#format':  '--dsjson'})
        self.assertEqual(isinstance(result, Job), True)
        self.assertEqual(len(result), 2)

        result = vw.train([self.input1, self.input2], [
            '--cb_explore_adf --dsjson --epsilon 0.1',
            '--cb_explore_adf --epsilon 0.2'])
        self.assertEqual(isinstance(result, list), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(isinstance(result[0], Job), True)       
        self.assertEqual(isinstance(result[1], Job), True) 
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 2)

        result = vw.train([self.input1, self.input2], [
            '--cb_explore_adf --dsjson --epsilon 0.1',
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])
        self.assertEqual(isinstance(result, list), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(isinstance(result[0], Job), True)       
        self.assertEqual(isinstance(result[1], Job), True) 
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 2)

        result = vw.train([self.input1, self.input2], [
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.1},
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])
        self.assertEqual(isinstance(result, list), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(isinstance(result[0], Job), True)       
        self.assertEqual(isinstance(result[1], Job), True) 
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 2)

        result = vw.train([self.input1, self.input2], Grid({
            '#base': ['--cb_explore_adf --dsjson'],
            '--epsilon': [0.1, 0.2]
        }))
        self.assertEqual(isinstance(result, list), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(isinstance(result[0], Job), True)       
        self.assertEqual(isinstance(result[1], Job), True) 
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 2)

    def test_data_shape_pandas(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.train(self.input1, pd.DataFrame(Grid([
            '--cb_explore_adf --dsjson --epsilon 0.1',
            '--cb_explore_adf --epsilon 0.2'])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 1)
        self.assertEqual(len(result.iloc[1]['!Job']), 1)

        result = vw.train(self.input1, pd.DataFrame(Grid([
            '--cb_explore_adf --dsjson --epsilon 0.1',
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 1)
        self.assertEqual(len(result.iloc[1]['!Job']), 1)

        result = vw.train(self.input1, pd.DataFrame(Grid([
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.1},
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 1)
        self.assertEqual(len(result.iloc[1]['!Job']), 1)

        result = vw.train(self.input1, pd.DataFrame(Grid({
            '#base': ['--cb_explore_adf --dsjson'],
            '--epsilon': [0.1, 0.2]
        })))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 1)
        self.assertEqual(len(result.iloc[1]['!Job']), 1)

        result = vw.train([self.input1, self.input2], pd.DataFrame(Grid([
            '--cb_explore_adf --dsjson --epsilon 0.1',
            '--cb_explore_adf --epsilon 0.2'])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 2)
        self.assertEqual(len(result.iloc[1]['!Job']), 2)

        result = vw.train([self.input1, self.input2], pd.DataFrame(Grid([
            '--cb_explore_adf --dsjson --epsilon 0.1',
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 2)
        self.assertEqual(len(result.iloc[1]['!Job']), 2)

        result = vw.train([self.input1, self.input2], pd.DataFrame(Grid([
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.1},
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 2)
        self.assertEqual(len(result.iloc[1]['!Job']), 2)

        result = vw.train([self.input1, self.input2], pd.DataFrame(Grid({
            '#base': ['--cb_explore_adf --dsjson'],
            '--epsilon': [0.1, 0.2]
        })))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 2)
        self.assertEqual(len(result.iloc[1]['!Job']), 2)

