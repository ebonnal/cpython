import math
from streamable import Stream

import time

def factors(n: int):
    return [i for i in range(1, math.floor(n // 2) + 1) if not n % i]
assert factors(10) == [1, 2, 5]

N = 1000000
if __name__ == "__main__":
    stream = Stream([N] * 100)
    # mono thread
    start_time = time.time()
    print("sum", sum(stream.map(factors).map(len)))
    print(time.time() - start_time)

    # concurrent 8 threads
    start_time = time.time()
    print("sum", sum(stream.map(factors, concurrency=8).map(len)))
    print(time.time() - start_time)

    # concurrent 8 processes
    start_time = time.time()
    print("sum", sum(stream.map(factors, concurrency=8, via_processes=True).map(len)))
    print(time.time() - start_time)

"""
./configure --disable-gil
make -j
sudo make install
"""