import unittest
from vw_executor.vw_cache import VwCache
from vw_executor.loggers import MultiLogger


class TestVwCache(unittest.TestCase):
    def test_equal_path_for_equal_options(self):
        opts1 = {'#cmd': '--ccb_explore_adf --dsjson --epsilon 0.1 -d file.txt'}
        opts2 = {'#blabla': '--dsjson --ccb_explore_adf',
                 '--epsilon': 0.1,
                 '-d': 'file.txt'}
        cache = VwCache('.vw_cache')

        logger = MultiLogger([])
        self.assertEqual(
            cache.get_path(opts1, logger),
            cache.get_path(opts2, logger))

    def test_non_equal_path_for_non_equal_options(self):
        opts1 = {'#cmd': '--ccb_explore_adf --dsjson --epsilon 0.1 -d file.txt'}
        opts2 = {'#blabla': '--ccb_explore_adf',
                 '--epsilon': 0.1,
                 '-d': 'file.txt'}
        cache = VwCache('.vw_cache')

        logger = MultiLogger([])
        self.assertNotEqual(
            cache.get_path(opts1, logger),
            cache.get_path(opts2, logger))

        self.assertNotEqual(
            cache.get_path(opts1, logger),
            cache.get_path(opts1, logger, '-p'))

        self.assertNotEqual(
            cache.get_path(opts1, logger, '-f'),
            cache.get_path(opts1, logger, '-p'))


if __name__ == '__main__':
    unittest.main()
