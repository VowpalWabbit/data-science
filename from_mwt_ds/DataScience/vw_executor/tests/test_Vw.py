import unittest
from vw_executor.vw import *
from vw_executor.vw_opts import Grid
import pandas as pd
from pathlib import Path
import shutil

def reset_cache_folder(path):
    p = Path(path)
    if p.exists() and p.is_dir():
        shutil.rmtree(p)

class TestVw(unittest.TestCase):
    input1 = 'vw_executor/tests/data/cb_0.json'
    input2 = 'vw_executor/tests/data/cb_1.json'
    input_ccb = 'vw_executor/tests/data/ccb_0.json'

    @classmethod
    def setUpClass(cls):
        import multiprocessing
        multiprocessing.set_start_method('spawn', force=True)

    def test_1file_1str_opt_train(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.train(self.input1, '--cb_explore_adf --dsjson')
        self.assertTrue(isinstance(result, Job))
        self.assertEqual(len(result), 1)
        self.assertIsNotNone(result.loss)

    def test_1file_1str_opt_test(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.test(self.input1, '--cb_explore_adf --dsjson')
        self.assertTrue(isinstance(result, Job))
        self.assertEqual(len(result), 1)
        self.assertIsNotNone(result.loss)       

    def test_1file_1dict_opt_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train(self.input1, {'#problem': '--cb_explore_adf', '#format':  '--dsjson'})
        self.assertTrue(isinstance(result, Job))
        self.assertEqual(len(result), 1)
        self.assertIsNotNone(result.loss)   

    def test_1file_1dict_opt_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test(self.input1, {'#problem': '--cb_explore_adf', '#format':  '--dsjson'})
        self.assertTrue(isinstance(result, Job))
        self.assertEqual(len(result), 1)
        self.assertIsNotNone(result.loss) 

    def test_1file_2str_opts_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train(self.input1, [
            '--cb_explore_adf --dsjson --epsilon 0.1',
            '--cb_explore_adf --dsjson --epsilon 0.2'])
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1) 
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)

    def test_1file_2str_opts_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test(self.input1, [
            '--cb_explore_adf --dsjson --epsilon 0.1',
            '--cb_explore_adf --dsjson --epsilon 0.2'])
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1) 
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)

    def test_1file_1str_1dict_opt_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train(self.input1, [
            '--cb_explore_adf --dsjson --epsilon 0.1',
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1)
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)

    def test_1file_1str_1dict_opt_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test(self.input1, [
            '--cb_explore_adf --dsjson --epsilon 0.1',
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1)
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)

    def test_1file_2dict_opts_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train(self.input1, [
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.1},
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1)
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)

    def test_1file_2dict_opts_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test(self.input1, [
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.1},
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1)
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)

    def test_1file_grid_opts_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train(self.input1, Grid({
            '#base': ['--cb_explore_adf --dsjson'],
            '--epsilon': [0.1, 0.2]
        }))
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1)      
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss) 

    def test_1file_grid_opts_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test(self.input1, Grid({
            '#base': ['--cb_explore_adf --dsjson'],
            '--epsilon': [0.1, 0.2]
        }))
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 1)
        self.assertEqual(len(result[1]), 1)      
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)

    def test_2files_1str_opt_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train([self.input1, self.input2], '--cb_explore_adf --dsjson')
        self.assertTrue(isinstance(result, Job))
        self.assertEqual(len(result), 2)
        self.assertIsNotNone(result.loss)

    def test_2files_1str_opt_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test([self.input1, self.input2], '--cb_explore_adf --dsjson')
        self.assertTrue(isinstance(result, Job))
        self.assertEqual(len(result), 2)
        self.assertIsNotNone(result.loss)    

    def test_2files_1dict_opt_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train([self.input1, self.input2], {'#problem': '--cb_explore_adf', '#format':  '--dsjson'})
        self.assertTrue(isinstance(result, Job))
        self.assertEqual(len(result), 2)
        self.assertIsNotNone(result.loss)

    def test_2files_1dict_opt_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test([self.input1, self.input2], {'#problem': '--cb_explore_adf', '#format':  '--dsjson'})
        self.assertTrue(isinstance(result, Job))
        self.assertEqual(len(result), 2)
        self.assertIsNotNone(result.loss)

    def test_2files_2str_opts_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train([self.input1, self.input2], [
            '--cb_explore_adf --dsjson --epsilon 0.1',
            '--cb_explore_adf --dsjson --epsilon 0.2'])
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 2)
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)

    def test_2files_2str_opts_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test([self.input1, self.input2], [
            '--cb_explore_adf --dsjson --epsilon 0.1',
            '--cb_explore_adf --dsjson --epsilon 0.2'])
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 2)
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)

    def test_2files_1str_1dict_opt_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train([self.input1, self.input2], [
            '--cb_explore_adf --dsjson --epsilon 0.1',
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 2)
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)

    def test_2files_1str_1dict_opt_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test([self.input1, self.input2], [
            '--cb_explore_adf --dsjson --epsilon 0.1',
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 2)
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)

    def test_2files_2dict_opts_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train([self.input1, self.input2], [
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.1},
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 2)
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)       

    def test_2files_2dict_opts_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test([self.input1, self.input2], [
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.1},
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 2)
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)       

    def test_2files_grid_opts_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train([self.input1, self.input2], Grid({
            '#base': ['--cb_explore_adf --dsjson'],
            '--epsilon': [0.1, 0.2]
        }))
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 2)
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)  

    def test_2files_grid_opts_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test([self.input1, self.input2], Grid({
            '#base': ['--cb_explore_adf --dsjson'],
            '--epsilon': [0.1, 0.2]
        }))
        self.assertTrue(isinstance(result, list))
        self.assertEqual(len(result), 2)
        self.assertTrue(isinstance(result[0], Job))       
        self.assertTrue(isinstance(result[1], Job)) 
        self.assertEqual(len(result[0]), 2)
        self.assertEqual(len(result[1]), 2)
        self.assertIsNotNone(result[0].loss)       
        self.assertIsNotNone(result[1].loss)

    def test_1file_2str_pd_opts_train(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.train(self.input1, pd.DataFrame(Grid([
            '--cb_explore_adf --dsjson --epsilon 0.1',
            '--cb_explore_adf --epsilon 0.2'])))
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 1)
        self.assertEqual(len(result.iloc[1]['!Job']), 1)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_1file_2str_pd_opts_test(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.test(self.input1, pd.DataFrame(Grid([
            '--cb_explore_adf --dsjson --epsilon 0.1',
            '--cb_explore_adf --epsilon 0.2'])))
        self.assertTrue(isinstance(result, pd.DataFrame))
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 1)
        self.assertEqual(len(result.iloc[1]['!Job']), 1)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_1file_1str_1dict_pd_opt_train(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.train(self.input1, pd.DataFrame(Grid([
            '--cb_explore_adf --dsjson --epsilon 0.1',
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 1)
        self.assertEqual(len(result.iloc[1]['!Job']), 1)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_1file_1str_1dict_pd_opt_test(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.test(self.input1, pd.DataFrame(Grid([
            '--cb_explore_adf --dsjson --epsilon 0.1',
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 1)
        self.assertEqual(len(result.iloc[1]['!Job']), 1)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_1file_2dict_pd_opts_train(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.train(self.input1, pd.DataFrame(Grid([
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.1},
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 1)
        self.assertEqual(len(result.iloc[1]['!Job']), 1)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_1file_2dict_pd_opts_test(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.test(self.input1, pd.DataFrame(Grid([
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.1},
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 1)
        self.assertEqual(len(result.iloc[1]['!Job']), 1)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_1file_grid_pd_opts_train(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.train(self.input1, pd.DataFrame(Grid({
            '#base': ['--cb_explore_adf --dsjson'],
            '--epsilon': [0.1, 0.2]
        })))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 1)
        self.assertEqual(len(result.iloc[1]['!Job']), 1)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_1file_grid_pd_opts_testW(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.test(self.input1, pd.DataFrame(Grid({
            '#base': ['--cb_explore_adf --dsjson'],
            '--epsilon': [0.1, 0.2]
        })))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 1)
        self.assertEqual(len(result.iloc[1]['!Job']), 1)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_2files_2str_pd_opts_train(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.train([self.input1, self.input2], pd.DataFrame(Grid([
            '--cb_explore_adf --dsjson --epsilon 0.1',
            '--cb_explore_adf --epsilon 0.2'])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 2)
        self.assertEqual(len(result.iloc[1]['!Job']), 2)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_2files_2str_pd_opts_test(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.test([self.input1, self.input2], pd.DataFrame(Grid([
            '--cb_explore_adf --dsjson --epsilon 0.1',
            '--cb_explore_adf --epsilon 0.2'])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 2)
        self.assertEqual(len(result.iloc[1]['!Job']), 2)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_2files_1str_1dict_pd_opt_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train([self.input1, self.input2], pd.DataFrame(Grid([
            '--cb_explore_adf --dsjson --epsilon 0.1',
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 2)
        self.assertEqual(len(result.iloc[1]['!Job']), 2)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_2files_1str_1dict_pd_opt_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test([self.input1, self.input2], pd.DataFrame(Grid([
            '--cb_explore_adf --dsjson --epsilon 0.1',
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 2)
        self.assertEqual(len(result.iloc[1]['!Job']), 2)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_2files_2dict_pd_opts_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train([self.input1, self.input2], pd.DataFrame(Grid([
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.1},
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 2)
        self.assertEqual(len(result.iloc[1]['!Job']), 2)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_2files_2dict_pd_opts_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test([self.input1, self.input2], pd.DataFrame(Grid([
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.1},
            {'#base': '--cb_explore_adf --dsjson', '--epsilon':  0.2}])))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 2)
        self.assertEqual(len(result.iloc[1]['!Job']), 2)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])
        
    def test_2files_grid_pd_opts_train(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.train([self.input1, self.input2], pd.DataFrame(Grid({
            '#base': ['--cb_explore_adf --dsjson'],
            '--epsilon': [0.1, 0.2]
        })))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 2)
        self.assertEqual(len(result.iloc[1]['!Job']), 2)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_2files_grid_pd_opts_test(self):
        vw = Vw('.vw_cache', handlers = [])

        result = vw.test([self.input1, self.input2], pd.DataFrame(Grid({
            '#base': ['--cb_explore_adf --dsjson'],
            '--epsilon': [0.1, 0.2]
        })))
        self.assertEqual(isinstance(result, pd.DataFrame), True)
        self.assertEqual(len(result), 2)
        self.assertEqual(len(result.iloc[0]['!Job']), 2)
        self.assertEqual(len(result.iloc[1]['!Job']), 2)
        self.assertIsNotNone(result.iloc[0]['!Loss'])       
        self.assertIsNotNone(result.iloc[1]['!Loss'])

    def test_e2e_test(self):
        cache = Path('.vw_cache_test')
        reset_cache_folder(cache)
        
        vw = Vw(cache, handlers = [])
        stdout_cache = cache.joinpath('cacheNone')

        result = vw.test(self.input1, '--cb_explore_adf --dsjson')
        self.assertEqual(len(list(cache.iterdir())), 1)        
        self.assertEqual(len(list(stdout_cache.iterdir())), 1)
        self.assertIsNotNone(result.loss)

        result = vw.test([self.input1, self.input2], '--cb_explore_adf --dsjson')
        self.assertEqual(len(list(cache.iterdir())), 1)        
        self.assertEqual(len(list(stdout_cache.iterdir())), 2)
        self.assertIsNotNone(result.loss)

        result = vw.test([self.input1, self.input2, self.input1], '--cb_explore_adf --dsjson')
        self.assertEqual(len(list(cache.iterdir())), 1)        
        self.assertEqual(len(list(stdout_cache.iterdir())), 2)
        self.assertIsNotNone(result.loss)

        result = vw._with(procs=1).test([self.input1, self.input2, self.input1], ['--cb_explore_adf --dsjson', '--cb_explore_adf --dsjson --epsilon 0.5'])
        self.assertEqual(len(list(cache.iterdir())), 1)        
        self.assertEqual(len(list(stdout_cache.iterdir())), 4)
        self.assertIsNotNone(result[0].loss)
        self.assertIsNotNone(result[1].loss)

    def test_e2e_train(self):
        cache = Path('.vw_cache_train')
        reset_cache_folder(cache)
        
        vw = Vw(cache, handlers = [])
        stdout_cache = cache.joinpath('cacheNone')
        model_cache = cache.joinpath('cache-f')

        result = vw.train(self.input1, '--cb_explore_adf --dsjson')
        self.assertEqual(len(list(cache.iterdir())), 2)        
        self.assertEqual(len(list(stdout_cache.iterdir())), 1)
        self.assertEqual(len(list(model_cache.iterdir())), 1)
        self.assertIsNotNone(result.loss)

        result = vw.train([self.input1, self.input2], '--cb_explore_adf --dsjson')
        self.assertEqual(len(list(cache.iterdir())), 2)        
        self.assertEqual(len(list(stdout_cache.iterdir())), 2)
        self.assertEqual(len(list(model_cache.iterdir())), 2)
        self.assertIsNotNone(result.loss)

        result = vw.train([self.input1, self.input2, self.input1], '--cb_explore_adf --dsjson')
        self.assertEqual(len(list(cache.iterdir())), 2)        
        self.assertEqual(len(list(stdout_cache.iterdir())), 3)
        self.assertEqual(len(list(model_cache.iterdir())), 3)
        self.assertIsNotNone(result.loss)

        result = vw._with(procs=1).train([self.input1, self.input2, self.input1], ['--cb_explore_adf --dsjson', '--cb_explore_adf --dsjson --epsilon 0.5'])
        self.assertEqual(len(list(cache.iterdir())), 2)        
        self.assertEqual(len(list(stdout_cache.iterdir())), 6)
        self.assertEqual(len(list(model_cache.iterdir())), 6)
        self.assertIsNotNone(result[0].loss)
        self.assertIsNotNone(result[1].loss)

    def test_failing_task(self):
        vw = Vw('.vw_cache', handlers = [])
        
        result = vw.train([self.input1, self.input_ccb], '--cb_explore_adf --dsjson --strict_parse')
        self.assertEqual(isinstance(result, Job), True)
        self.assertEqual(len(result), 2)
        self.assertIsNone(result.loss)
        self.assertIsNotNone(result[0].loss)
        self.assertIsNone(result[1].loss)
        self.assertEqual(result[1], result.failed)


