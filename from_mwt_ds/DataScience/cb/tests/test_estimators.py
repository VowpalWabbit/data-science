import unittest

from Pipeline.cb.estimators import ips_snips

class ips_snips_test(unittest.TestCase):
    def test_0(self):
        e = ips_snips()
        self.assertEqual(e.get('ips'), 0)
        self.assertEqual(e.get('snips'), 0)
        self.assertEqual(e.get_interval(), (0, 0))

    def test_1(self):
        e = ips_snips()
        e.add(1, 0.8, 0.2)
        self.assertEqual(e.get('ips'), 0.25)
        self.assertEqual(e.get('snips'), 1)

if __name__=='__main__':
    unittest.main()
