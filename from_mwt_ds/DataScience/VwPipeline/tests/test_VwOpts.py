import unittest
from VwPipeline import VwOpts


class TestStringHash(unittest.TestCase):
    def test_equal_after_normalize(self):
        self.assertEqual(
            VwOpts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            VwOpts.string_hash(' --ccb_explore_adf --dsjson --epsilon 0.1'))

        self.assertEqual(
            VwOpts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            VwOpts.string_hash('--dsjson  --ccb_explore_adf  --epsilon 0.1  '))

        self.assertEqual(
            VwOpts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.2'),
            VwOpts.string_hash('--dsjson  --ccb_explore_adf --l 0.2 --epsilon 0.1  '))

    def test_not_equal_after_normalize(self):
        self.assertNotEqual(
            VwOpts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            VwOpts.string_hash(' --ccb_explore_adf --dsjson --epsilon 0.2'))

        self.assertNotEqual(
            VwOpts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            VwOpts.string_hash('--dsjson  --cb_explore_adf  --epsilon 0.1  '))

        self.assertNotEqual(
            VwOpts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.2'),
            VwOpts.string_hash('--dsjson  --ccb_explore_adf --l 0.1 --epsilon 0.2  '))


class TestToString(unittest.TestCase):
    def test_to_string(self):
        self.assertEqual(
            VwOpts.to_string({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson'}),
            '--ccb_explore_adf --epsilon 0.1 --dsjson')

        self.assertEqual(
            VwOpts.to_string({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson',
                              '--l': 0.1}),
            '--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.1')

        self.assertEqual(
            VwOpts.to_string({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson',
                              '--l': 0.1,
                              '--cb_type': 'mtr'}),
            '--ccb_explore_adf --epsilon 0.1 --dsjson --cb_type mtr --l 0.1')


class TestDimension(unittest.TestCase):
    def test_dimension(self):
        self.assertEqual(
            VwOpts.dimension('-o', []),
            [])

        self.assertEqual(
            VwOpts.dimension('-o', [1, 2, 3]),
            [{'-o': '1'}, {'-o': '2'}, {'-o': '3'}])

        self.assertEqual(
            VwOpts.dimension('-o', ['value1', 'value2', 'value3']),
            [{'-o': 'value1'}, {'-o': 'value2'}, {'-o': 'value3'}])


class TestProduct(unittest.TestCase):
    def test_product(self):
        self.assertEqual(
            VwOpts.product(
                VwOpts.dimension('-o1', []),
                VwOpts.dimension('-o2', [])
            ),
            [])

        self.assertEqual(
            VwOpts.product(
                VwOpts.dimension('-o1', [1, 2, 3]),
                VwOpts.dimension('-o2', [])
            ),
            [])

        self.assertEqual(
            VwOpts.product(
                VwOpts.dimension('-o1', []),
                VwOpts.dimension('-o2', [1, 2, 3])
            ),
            [])

        self.assertEqual(
            VwOpts.product(
                VwOpts.dimension('-o1', [1, 2, 3]),
                [{}]
            ),
            VwOpts.dimension('-o1', [1, 2, 3]))

        self.assertEqual(
            VwOpts.product(
                [{}],
                VwOpts.dimension('-o2', [1, 2, 3])
            ),
            VwOpts.dimension('-o2', [1, 2, 3]))

        self.assertEqual(
            VwOpts.product(
                VwOpts.dimension('-o1', [1, 2]),
                VwOpts.dimension('-o2', [1, 2])
            ),
            [{'-o1': '1', '-o2': '1'}, {'-o1': '1', '-o2': '2'}, {'-o1': '2', '-o2': '1'}, {'-o1': '2', '-o2': '2'}])


class TestCacheCmd(unittest.TestCase):
    def test_cache_cmd_generation(self):
        self.assertEqual(
            VwOpts.to_cache_cmd({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson'}),
            '--ccb_explore_adf --dsjson')

        self.assertEqual(
            VwOpts.to_cache_cmd({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson',
                                 '-b': 20}),
            '--ccb_explore_adf --dsjson -b 20')

        self.assertEqual(
            VwOpts.to_cache_cmd({'#base': '--compressed --cb_explore_adf --epsilon 0.1',
                                 '--bit_precision': 20,
                                 '--cb_type': 'mtr'}),
            '--cb_explore_adf --compressed -b 20')


if __name__ == '__main__':
    unittest.main()
