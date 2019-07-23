import numpy as np
from .run_pyspark import PySparkMgr
from .redis_db import RedisDBWrapper
from .fast_kmeans import FastKmeans
from .bk_tree import BKTNode, BKTree


def fvecs_read(filename, c_contiguous=True):
    fv = np.fromfile(filename, dtype=np.float32)
    if fv.size == 0:
        return np.zeros((0, 0))
    dim = fv.view(np.int32)[0]
    assert dim > 0
    fv = fv.reshape(-1, 1 + dim)
    if not all(fv.view(np.int32)[:, 0] == dim):
        raise IOError("Non-uniform vector sizes in " + filename)
    fv = fv[:, 1:]
    if c_contiguous:
        fv = fv.copy()
    return fv


if __name__ == "__main__":
    spark_args = {}
    spark_args["spark.driver.memory"] = '10g'
    spark_args["spark.executor.memory"] = '10g'
    spark_args["spark.executor.cores"] = str(4)
    spark_args["spark.executor.instances"] = str(1)

    pysparkmgr = PySparkMgr(spark_args)
    spark, sc = pysparkmgr.start()

    if not spark:
        print("Spark session failed to launch")
        exit()

    data = fvecs_read('sift/sift_learn.fvecs').astype(np.float64)

    RedisDBWrapper.CHUNK_SIZE = int(RedisDBWrapper.MAX_CHUNK_SIZE / data.shape[1])

    bkt = BKTree(max_clusters_per_run = 10, max_depth = 5, min_cluster_size = 2500, sc = sc, redis_host = "10.10.50.32")

    bkt.build(data)

    bkt.get_nearest_cluster(data[0])
