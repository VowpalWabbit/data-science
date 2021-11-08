import unittest
from vw_executor.vw_opts import VwOpts, dimension, product, Grid

def assertGridEqualsList(tc, actual: Grid, expected: list):
    expected = list([dict(o) for o in expected])
    tc.assertEqual(len(actual), len(expected))
    for e in expected:
        found = False
        for a in actual:
            if dict(e) == dict(a):
                found = True
                break
        tc.assertEqual(found, True)


class TestStringHash(unittest.TestCase):
    def test_equal_after_normalize(self):
        self.assertEqual(
            VwOpts('--ccb_explore_adf --epsilon 0.1 --dsjson').hash(),
            VwOpts(' --ccb_explore_adf --dsjson --epsilon 0.1').hash())

        self.assertEqual(
            VwOpts('--ccb_explore_adf --epsilon 0.1 --dsjson').hash(),
            VwOpts('--dsjson  --ccb_explore_adf  --epsilon 0.1  ').hash())

        self.assertEqual(
            VwOpts('--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.2').hash(),
            VwOpts('--dsjson  --ccb_explore_adf --l 0.2 --epsilon 0.1  ').hash())


    def test_not_equal_after_normalize(self):
        self.assertNotEqual(
            VwOpts('--ccb_explore_adf --epsilon 0.1 --dsjson').hash(),
            VwOpts(' --ccb_explore_adf --dsjson --epsilon 0.2').hash())

        self.assertNotEqual(
            VwOpts('--ccb_explore_adf --epsilon 0.1 --dsjson').hash(),
            VwOpts('--dsjson  --cb_explore_adf  --epsilon 0.1  ').hash())

        self.assertNotEqual(
            VwOpts('--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.2').hash(),
            VwOpts('--dsjson  --ccb_explore_adf --l 0.1 --epsilon 0.2  ').hash())


class TestToString(unittest.TestCase):
    def test_to_string(self):
        self.assertEqual(
            str(VwOpts({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson'})),
            '--ccb_explore_adf --epsilon 0.1 --dsjson')

        self.assertEqual(
            str(VwOpts({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson',
                              '--l': 0.1})),
            '--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.1')

        self.assertEqual(
            str(VwOpts({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson',
                              '--l': 0.1,
                              '--cb_type': 'mtr'})),
            '--ccb_explore_adf --epsilon 0.1 --dsjson --cb_type mtr --l 0.1')


class TestDimension(unittest.TestCase):
    def test_dimension(self):
        assertGridEqualsList(self,
            dimension('-o', []),
            [])

        assertGridEqualsList(self,
            dimension('-o', [1, 2, 3]),
            [{'-o': 1}, {'-o': 2}, {'-o': 3}])

        assertGridEqualsList(self,
            dimension('-o', ['value1', 'value2', 'value3']),
            [{'-o': 'value1'}, {'-o': 'value2'}, {'-o': 'value3'}])


class TestProduct(unittest.TestCase):
    def test_product(self):
        assertGridEqualsList(self,
            product(
                dimension('-o1', []),
                dimension('-o2', [])
            ),
            [])

        assertGridEqualsList(self,
            product(
                dimension('-o1', [1, 2, 3]),
                dimension('-o2', [])
            ),
            [])

        assertGridEqualsList(self,
            product(
                dimension('-o1', []),
                dimension('-o2', [1, 2, 3])
            ),
            [])

        assertGridEqualsList(self,
            product(
                dimension('-o1', [1, 2, 3]),
                [{}]
            ),
            dimension('-o1', [1, 2, 3]))

        assertGridEqualsList(self,
            product(
                [{}],
                dimension('-o2', [1, 2, 3])
            ),
            dimension('-o2', [1, 2, 3]))

        assertGridEqualsList(self,
            product(
                dimension('-o1', [1, 2]),
                dimension('-o2', [1, 2])
            ),
            [{'-o1': 1, '-o2': 1}, {'-o1': 1, '-o2': 2}, {'-o1': 2, '-o2': 1}, {'-o1': 2, '-o2': 2}])


class TestCacheCmd(unittest.TestCase):
    def test_cache_cmd_generation(self):
        self.assertEqual(
            VwOpts({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson'}).to_cache_cmd(),
            '--ccb_explore_adf --dsjson')

        self.assertEqual(
            VwOpts({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson',
                                 '-b': 20}).to_cache_cmd(),
            '--ccb_explore_adf --dsjson -b 20')

        self.assertEqual(
            VwOpts({'#base': '--compressed --cb_explore_adf --epsilon 0.1',
                                 '--bit_precision': 20,
                                 '--cb_type': 'mtr'}).to_cache_cmd(),
            '--cb_explore_adf --compressed -b 20')

class TestGrid(unittest.TestCase):
    def test_grid_construction(self):
        assertGridEqualsList(self, Grid([]), [])
        assertGridEqualsList(self,
            Grid(['--cb_explore_adf']),
            [{'#0': '--cb_explore_adf'}])
        assertGridEqualsList(self,
            Grid([{'#algo': '--cb_explore_adf', '#format': '--dsjson'}]),
            [{'#algo': '--cb_explore_adf', '#format': '--dsjson'}])
        assertGridEqualsList(self,
            Grid([{'#algo': '--cb_explore_adf', '#format': '--dsjson'}, '--cb_explore_adf']),
            [{'#algo': '--cb_explore_adf', '#format': '--dsjson'}, {'#0': '--cb_explore_adf'}])
        assertGridEqualsList(self,
            Grid({'a': [1,2], 'b': [3,4]}), 
            [{'a':1,'b':3},{'a':1,'b':4},{'a':2,'b':3},{'a':2,'b':4}])

    def test_grid_product(self):
        self.assertEqual(
            Grid([]) * Grid([]),
            Grid([]))
        self.assertEqual(
            Grid(['--cb_explore_adf']) * Grid([]),
            Grid([]))
        self.assertEqual(
            Grid([]) * Grid(['--cb_explore_adf']),
            Grid([]))
        self.assertEqual(
            Grid({'a': [1,2]}) * Grid({'b': [3,4]}),
            Grid({'a': [1,2], 'b': [3,4]}))

    def test_grid_sum(self):
        self.assertEqual(
            Grid([]) + Grid([]),
            Grid([]))
        self.assertEqual(
            Grid(['--cb_explore_adf']) + Grid([]),
            Grid(['--cb_explore_adf']))
        self.assertEqual(
            Grid([]) + Grid(['--cb_explore_adf']),
            Grid(['--cb_explore_adf']))
        self.assertEqual(
            Grid(['--cb_explore_adf']) + Grid(['--cb_explore_adf']),
            Grid(['--cb_explore_adf']))
        self.assertEqual(
            Grid(['--dsjson --cb_explore_adf']) + Grid([{'#algo': '--cb_explore_adf', '#format': '--dsjson'}]),
            Grid(['--cb_explore_adf --dsjson']))  
        self.assertEqual(
            Grid({'a': [1, 2], 'b': [3,4]}),
            Grid({'a': [1,2]}) * (Grid({'b': [3]}) + Grid({'b': [4]})))
           

if __name__ == '__main__':
    unittest.main()
