from concurrent.futures import ProcessPoolExecutor, ThreadPoolExecutor

concurrency = 8
def printing_identity(_):
    print(_)
    return _
if __name__ == "__main__":
    with ProcessPoolExecutor(concurrency) as executor:
        it = executor.map(int, map(printing_identity, range(15)), buffersize=4)
        print("next", next(it), "endnext")
        print("next", next(it), "endnext")