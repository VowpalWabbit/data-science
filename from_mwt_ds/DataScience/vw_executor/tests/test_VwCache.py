import unittest
from vw_executor.vw_cache import VwCache


class TestVwCache(unittest.TestCase):
    def test_equal_path_for_equal_options(self):
        opts1 = {'#cmd': '--ccb_explore_adf --dsjson --epsilon 0.1 -d file.txt'}
        opts2 = {'#blabla': '--dsjson --ccb_explore_adf',
                 '--epsilon': 0.1,
                 '-d': 'file.txt'}
        cache = VwCache('.vw_cache')

        self.assertEqual(
            cache.get_path(opts1),
            cache.get_path(opts2))

    def test_non_equal_path_for_non_equal_options(self):
        opts1 = {'#cmd': '--ccb_explore_adf --dsjson --epsilon 0.1 -d file.txt'}
        opts2 = {'#blabla': '--ccb_explore_adf',
                 '--epsilon': 0.1,
                 '-d': 'file.txt'}
        cache = VwCache('.vw_cache')

        self.assertNotEqual(
            cache.get_path(opts1),
            cache.get_path(opts2))

        self.assertNotEqual(
            cache.get_path(opts1),
            cache.get_path(opts1, '-p'))

        self.assertNotEqual(
            cache.get_path(opts1, '-f'),
            cache.get_path(opts1, '-p'))


if __name__ == '__main__':
    unittest.main()
