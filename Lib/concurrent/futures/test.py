from thread import ThreadPoolExecutor
import time
from typing import List

concurrency = 8
with ThreadPoolExecutor(max_workers=concurrency) as executor:
    l: List[int] = []
    executor.map(l.append, range(1000))
    print(l)
    time.sleep(1)
    assert len(l) <= concurrency + 1