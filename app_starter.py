from strategy.duplicate_finder import (
    DetectDuplicateWithBloomFilter,
    DetectDuplicateWithHashMap,
)
from time import perf_counter


if __name__ == "__main__":

    app = DetectDuplicateWithBloomFilter(3)
    #app = DetectDuplicateWithHashMap()

    t1_start = perf_counter()
    app.start_reader()
    app.cleanup()
    t1_stop = perf_counter()
    print("Elapsed time during the whole program in seconds:", t1_stop - t1_start)
