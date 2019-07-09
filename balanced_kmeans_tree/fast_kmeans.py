import random
import time
import numpy as np
from .redis_db import RedisDBWrapper

class FastKmeans(object):

    __iterations = 1000
    __converge_threshold = 0.1

    #计算每个分区中的数据应该分配到的新cluster，新cluster的数据总和以及计数
    @staticmethod
    def assign_to_new_centroids(iterator, centroids):
        centroids = centroids.value
        points = np.array([x for x in iterator])
        if len(points) == 0:
            return [(k, np.zeros(shape = (1, centroids.shape[1])), 0) for k in range(centroids.shape[0])]

        distances = np.sqrt(((points - centroids[:, np.newaxis]) ** 2).sum(axis = 2))
        closest =  np.argmin(distances, axis = 0)

        sum = np.array([points[closest == k].sum(axis = 0) for k in range(centroids.shape[0])])
        count = np.array([len(points[closest == k]) for k in range(centroids.shape[0])])

        for k in range(centroids.shape[0]):
            yield (k, sum[k], count[k])


    #计算新的cluster中心
    @staticmethod
    def move_to_new_centroids(collect_data):

        centors_map = {}
        for cid, sum, size in collect_data:
            if cid not in centors_map:
                centors_map[cid] = ([sum], [size])
            else:
                centors_map[cid][0].append(sum)
                centors_map[cid][1].append(size)

        centroids = []
        cluster_sizes = []

        for k, v in centors_map.items():
            centroids.append(np.array(v[0]).sum(axis = 0) / np.array(v[1]).sum())
            cluster_sizes.append(np.array(v[1]).sum())

        return np.array(centroids), np.array(cluster_sizes)


    @staticmethod
    def initialize_centroids(points, k):
        """returns k centroids from the initial points"""
        centers_id = random.sample(range(points.shape[0]), k)
        return points[centers_id]

    @staticmethod
    def center_diff(a, b):
        d = a - b

        return (d * d).sum()

    @staticmethod
    def assign_to_final_centroids(iterator, centroids):
        points = np.array([x for x in iterator])
        if len(points) == 0:
            return [(k, []) for k in range(centroids.shape[0])]

        distances = np.sqrt(((points - centroids[:, np.newaxis]) ** 2).sum(axis = 2))
        closest =  np.argmin(distances, axis = 0)

        for k in range(centroids.shape[0]):
            yield (k, points[closest == k])

    @staticmethod
    def save_cluster_to_redis(cid, iterator, shape_w, redis_host, redis_port, cluster_key):
        points = np.zeros(shape = (1, shape_w))
        for x in iterator:
            points = np.concatenate((points, x), axis = 0)

        r = RedisDBWrapper(host = redis_host, port = redis_port)

        ret = r.save_data(points[1:], cluster_key + '_' + str(cid))

        return (cid, points.sum(axis = 0) / (len(points) - 1), len(points) - 1, ret)

    @staticmethod
    def fit(data, k, redis_host, redis_port = 6379, sc = None, cluster_key = None):
        if not cluster_key:
            cluster_key = str(int(time.mktime(time.localtime())))

        n_centors = FastKmeans.initialize_centroids(data, k)
        n_centors = n_centors[n_centors[:, 0].argsort()]

        centors = n_centors

        rdd = sc.parallelize(data)

        for i in range(FastKmeans.__iterations):
            #print("round %d ..."%i)

            broadcast_centors = sc.broadcast(n_centors)

            collect_data = rdd.mapPartitions(lambda x: FastKmeans.assign_to_new_centroids(x, broadcast_centors)).collect()
            n_centors, cluster_sizes = FastKmeans.move_to_new_centroids(collect_data)
            n_centors = n_centors[n_centors[:, 0].argsort()]

            if FastKmeans.center_diff(n_centors, centors) < FastKmeans.__converge_threshold:
                break

            centors = n_centors

        if redis_host:
            result = rdd.mapPartitions(lambda x: FastKmeans.assign_to_final_centroids(x, n_centors)).groupByKey().\
                         map(lambda x: FastKmeans.save_cluster_to_redis(x[0], x[1], n_centors.shape[1], redis_host, redis_port, cluster_key)).collect()

        return result
