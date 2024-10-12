from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor
from multiprocessing import Manager
import time
import unittest

from .executor import ExecutorTest
from .util import BaseTestCase, setup_module


class PoolExecutorTest(ExecutorTest, BaseTestCase):
    def test_map_buffersize(self):
        manager = Manager()
        for ExecutorType in (ThreadPoolExecutor, ProcessPoolExecutor):
            with ExecutorType(max_workers=1) as pool:
                with self.assertRaisesRegex(
                    ValueError, "buffersize must be None or >= 1."
                ):
                    pool.map(bool, [], buffersize=0)

            for buffersize in [1, 5]:
                iterable = range(10)
                processed_elements = manager.list()

                with ExecutorType(max_workers=1) as pool:
                    iterator = pool.map(
                        processed_elements.append, iterable, buffersize=buffersize
                    )
                    time.sleep(0.2)  # wait for buffered futures to finish
                    self.assertSetEqual(set(processed_elements), set(range(buffersize)))
                    next(iterator)
                    time.sleep(0.1)  # wait for the created future to finish
                    self.assertSetEqual(
                        set(processed_elements), set(range(buffersize + 1))
                    )


def setUpModule():
    setup_module()


if __name__ == "__main__":
    unittest.main()
