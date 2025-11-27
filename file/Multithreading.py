import concurrent.futures as future


pool_threads = None


def get_pool_threads(max=None):
    global pool_threads
    if pool_threads is None:
        pool_threads = future.ThreadPoolExecutor(max)
    return pool_threads