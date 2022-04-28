from vw_executor.loggers import MultiFileLogger, FileLogger
from pathlib import Path
import shutil

import unittest

class TestMultiFileLogger(unittest.TestCase):
    def setUp(self):
        if Path('test_logs').exists():
            shutil.rmtree('test_logs')

    def test_nesting_is_supported(self):
        root = MultiFileLogger('test_logs', 'DEBUG')
        root.debug('root')

        level1_a = root['a']
        level1_a.debug('level1_a')

        level2_a = level1_a['a']
        level2_a.debug('level2_a')   

        level2_b = level1_a['b']
        level2_b.debug('level2_b')

        level1_b = root['b']
        level1_b.debug('level1_b')      

        with open('test_logs/log.txt') as f:
            content = f.readlines()
            self.assertEqual(len(content), 1)
            self.assertTrue(content[0].strip().endswith('root'))  

        with open('test_logs/a/log.txt') as f:
            content = f.readlines()
            self.assertEqual(len(content), 1)
            self.assertTrue(content[0].strip().endswith('level1_a'))

        with open('test_logs/b/log.txt') as f:
            content = f.readlines()
            self.assertEqual(len(content), 1)
            self.assertTrue(content[0].strip().endswith('level1_b'))  

        with open('test_logs/a/a/log.txt') as f:
            content = f.readlines()
            self.assertEqual(len(content), 1)
            self.assertTrue(content[0].strip().endswith('level2_a'))

        with open('test_logs/a/b/log.txt') as f:
            content = f.readlines()
            self.assertEqual(len(content), 1)
            self.assertTrue(content[0].strip().endswith('level2_b'))  


class TestFileLogger(unittest.TestCase):
    def setUp(self):
        self.log_path = 'test_logs/single_log.txt'
        if Path(self.log_path).exists():
            Path(self.log_path).unlink()

    def test_nesting_is_supported(self):
        root = FileLogger(self.log_path, 'DEBUG')
        root.debug('root')

        level1_a = root['a']
        level1_a.debug('level1_a')

        level2_a = level1_a['a']
        level2_a.debug('level2_a')   

        level2_b = level1_a['b']
        level2_b.debug('level2_b')

        level1_b = root['b']
        level1_b.debug('level1_b')      

        def trim_time(line):
            return line[line.find(']')+1:]

        with open(self.log_path) as f:
            content = f.readlines()
            self.assertEqual(len(content), 5)
            self.assertTrue(trim_time(content[0]), ' root') 
            self.assertTrue(trim_time(content[1]), '[a] level1_a')  
            self.assertTrue(trim_time(content[2]), '[a][a] level2_a') 
            self.assertTrue(trim_time(content[3]), '[a][b] level2_b')  
            self.assertTrue(trim_time(content[4]), '[b] level1_b') 
