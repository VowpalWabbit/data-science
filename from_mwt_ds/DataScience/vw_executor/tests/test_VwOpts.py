import unittest
from vw_executor.vw_opts import VwOpts, dimension, product


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
        self.assertEqual(
            dimension('-o', []),
            [])

        self.assertEqual(
            dimension('-o', [1, 2, 3]),
            list({VwOpts({'-o': 1}), VwOpts({'-o': 2}), VwOpts({'-o': 3})}))

        self.assertEqual(
            dimension('-o', ['value1', 'value2', 'value3']),
            list({VwOpts({'-o': 'value1'}), VwOpts({'-o': 'value2'}), VwOpts({'-o': 'value3'})}))


class TestProduct(unittest.TestCase):
    def test_product(self):
        self.assertEqual(
            product(
                dimension('-o1', []),
                dimension('-o2', [])
            ),
            [])

        self.assertEqual(
            product(
                dimension('-o1', [1, 2, 3]),
                dimension('-o2', [])
            ),
            [])

        self.assertEqual(
            product(
                dimension('-o1', []),
                dimension('-o2', [1, 2, 3])
            ),
            [])

        self.assertEqual(
            product(
                dimension('-o1', [1, 2, 3]),
                [{}]
            ),
            dimension('-o1', [1, 2, 3]))

        self.assertEqual(
            product(
                [{}],
                dimension('-o2', [1, 2, 3])
            ),
            dimension('-o2', [1, 2, 3]))

        self.assertEqual(
            product(
                dimension('-o1', [1, 2]),
                dimension('-o2', [1, 2])
            ),
            list({VwOpts({'-o1': 1, '-o2': 1}), VwOpts({'-o1': 1, '-o2': 2}), VwOpts({'-o1': 2, '-o2': 1}), VwOpts({'-o1': 2, '-o2': 2})}))


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


if __name__ == '__main__':
    unittest.main()
