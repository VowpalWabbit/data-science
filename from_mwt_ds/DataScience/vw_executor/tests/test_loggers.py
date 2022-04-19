from vw_executor.loggers import MultiFileLogger
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
