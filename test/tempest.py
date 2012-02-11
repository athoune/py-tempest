import unittest
import sys
import os

sys.path.insert(0, os.path.dirname(__file__) + '/../src')

from tempest import Cluster


class TestWork(unittest.TestCase):

    def setUp(self):
        app = Cluster()
        self.worker = app.worker('test')

    def test_work(self):

        @self.worker.on('test')
        def test(context, age, name):
            context.stop()
            self.assertEqual(42, age)
            self.assertEqual('Bob', name)
            return "Hello %s!" % name

        self.worker.work.test(None, 42, 'Bob')
        self.worker.run()

if __name__ == '__main__':
    unittest.main()
