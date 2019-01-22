# -*- coding: utf-8 -*-

import unittest
import warnings
import importlib
import sys
sys.path.append('../')


def execute_valid_process():
        module = importlib.import_module('dataPrepartion.dataIngestion')
        print(module)
        prcs = "prc_PrcId_[0-9].json"
        pool = 3
        # module.main(args.config, args.prcs, args.pool)
        module.main('config\\config.cnf', prcs, pool)
        


class Test(unittest.TestCase):
    
  
    
    def setUp(self):
        warnings.simplefilter('ignore', category=ImportWarning)
        warnings.simplefilter('ignore', category=DeprecationWarning)
        execute_valid_process()

    def test_execute_valid_process(self):
        print("The return status is")
        self.assertTrue(True)

    def test_02(self):

        self.assertFalse(False)


if __name__ == '__main__':
    unittest.main()