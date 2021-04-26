import unittest

from Pipeline.cb.estimators import ips_snips
from Pipeline.ccb.estimators import cb_estimator

class cb_estimator_test(unittest.TestCase):
    def test_0(self):
        e = cb_estimator(ips_snips)
        self.assertEqual(e.get(weights=lambda i, s: int(i==0), type='ips'), 0)

    def test_1(self):
        e = cb_estimator(ips_snips)
        e.add([1,4,16], [1, 1, 1], [1, 0.5, 0.25])
        ips0 = e.get(weights=lambda i, s: int(i==0), type='ips')
        ips1 = e.get(weights=lambda i, s: int(i==1), type='ips')
        ips2 = e.get(weights=lambda i, s: int(i==2), type='ips')
        self.assertEqual(ips0, 1)
        self.assertEqual(ips1, 2)
        self.assertEqual(ips2, 4)

    def test_2(self):
        e = cb_estimator(ips_snips)
        e.add([1,4,16], [1, 1, 1], [1, 0.5, 0.25])
        ips0 = e.get(weights=lambda i, s: int(i==0), type='ips')
        ips1 = e.get(weights=lambda i, s: int(i==1), type='ips')
        ips2 = e.get(weights=lambda i, s: int(i==2), type='ips')
        self.assertEqual(ips0, 1)
        self.assertEqual(ips1, 2)
        self.assertEqual(ips2, 4)

        e2 = e + e
        ips0 = e2.get(weights=lambda i, s: int(i==0), type='ips')
        ips1 = e2.get(weights=lambda i, s: int(i==1), type='ips')
        ips2 = e2.get(weights=lambda i, s: int(i==2), type='ips')
        self.assertEqual(ips0, 1)
        self.assertEqual(ips1, 2)
        self.assertEqual(ips2, 4)

if __name__=='__main__':
    unittest.main()
