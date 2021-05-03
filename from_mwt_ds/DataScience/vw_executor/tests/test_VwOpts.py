import unittest
from vw_executor import vw_opts


class TestStringHash(unittest.TestCase):
    def test_equal_after_normalize(self):
        self.assertEqual(
            vw_opts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            vw_opts.string_hash(' --ccb_explore_adf --dsjson --epsilon 0.1'))

        self.assertEqual(
            vw_opts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            vw_opts.string_hash('--dsjson  --ccb_explore_adf  --epsilon 0.1  '))

        self.assertEqual(
            vw_opts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.2'),
            vw_opts.string_hash('--dsjson  --ccb_explore_adf --l 0.2 --epsilon 0.1  '))

    def test_not_equal_after_normalize(self):
        self.assertNotEqual(
            vw_opts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            vw_opts.string_hash(' --ccb_explore_adf --dsjson --epsilon 0.2'))

        self.assertNotEqual(
            vw_opts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson'),
            vw_opts.string_hash('--dsjson  --cb_explore_adf  --epsilon 0.1  '))

        self.assertNotEqual(
            vw_opts.string_hash('--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.2'),
            vw_opts.string_hash('--dsjson  --ccb_explore_adf --l 0.1 --epsilon 0.2  '))


class TestToString(unittest.TestCase):
    def test_to_string(self):
        self.assertEqual(
            vw_opts.to_string({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson'}),
            '--ccb_explore_adf --epsilon 0.1 --dsjson')

        self.assertEqual(
            vw_opts.to_string({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson',
                              '--l': 0.1}),
            '--ccb_explore_adf --epsilon 0.1 --dsjson --l 0.1')

        self.assertEqual(
            vw_opts.to_string({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson',
                              '--l': 0.1,
                              '--cb_type': 'mtr'}),
            '--ccb_explore_adf --epsilon 0.1 --dsjson --cb_type mtr --l 0.1')


class TestDimension(unittest.TestCase):
    def test_dimension(self):
        self.assertEqual(
            vw_opts.dimension('-o', []),
            [])

        self.assertEqual(
            vw_opts.dimension('-o', [1, 2, 3]),
            [{'-o': 1}, {'-o': 2}, {'-o': 3}])

        self.assertEqual(
            vw_opts.dimension('-o', ['value1', 'value2', 'value3']),
            [{'-o': 'value1'}, {'-o': 'value2'}, {'-o': 'value3'}])


class TestProduct(unittest.TestCase):
    def test_product(self):
        self.assertEqual(
            vw_opts.product(
                vw_opts.dimension('-o1', []),
                vw_opts.dimension('-o2', [])
            ),
            [])

        self.assertEqual(
            vw_opts.product(
                vw_opts.dimension('-o1', [1, 2, 3]),
                vw_opts.dimension('-o2', [])
            ),
            [])

        self.assertEqual(
            vw_opts.product(
                vw_opts.dimension('-o1', []),
                vw_opts.dimension('-o2', [1, 2, 3])
            ),
            [])

        self.assertEqual(
            vw_opts.product(
                vw_opts.dimension('-o1', [1, 2, 3]),
                [{}]
            ),
            vw_opts.dimension('-o1', [1, 2, 3]))

        self.assertEqual(
            vw_opts.product(
                [{}],
                vw_opts.dimension('-o2', [1, 2, 3])
            ),
            vw_opts.dimension('-o2', [1, 2, 3]))

        self.assertEqual(
            vw_opts.product(
                vw_opts.dimension('-o1', [1, 2]),
                vw_opts.dimension('-o2', [1, 2])
            ),
            [{'-o1': 1, '-o2': 1}, {'-o1': 1, '-o2': 2}, {'-o1': 2, '-o2': 1}, {'-o1': 2, '-o2': 2}])


class TestCacheCmd(unittest.TestCase):
    def test_cache_cmd_generation(self):
        self.assertEqual(
            vw_opts.to_cache_cmd({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson'}),
            '--ccb_explore_adf --dsjson')

        self.assertEqual(
            vw_opts.to_cache_cmd({'#base': '--ccb_explore_adf --epsilon 0.1 --dsjson',
                                 '-b': 20}),
            '--ccb_explore_adf --dsjson -b 20')

        self.assertEqual(
            vw_opts.to_cache_cmd({'#base': '--compressed --cb_explore_adf --epsilon 0.1',
                                 '--bit_precision': 20,
                                 '--cb_type': 'mtr'}),
            '--cb_explore_adf --compressed -b 20')


if __name__ == '__main__':
    unittest.main()
