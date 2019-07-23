import random
import time
import numpy as np
from .redis_db import RedisDBWrapper

class FastKmeans(object):

    __iterations = 1000
    __converge_threshold = 0.1
    __MAX_RETRY_TIMES = 5


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
            if np.array(v[1]).sum() == 0:
                return None, None

            centroids.append(np.array(v[0]).sum(axis = 0) / np.array(v[1]).sum())
            cluster_sizes.append(np.array(v[1]).sum())

        return np.array(centroids), np.array(cluster_sizes)

    @staticmethod
    def get_closest_distance(iterator, centroids):
        centroids = centroids.value

        data = np.array([x for x in iterator])
        np.random.shuffle(data)

        points = np.array([x[0] for x in data])
        points_idx = np.array([x[1] for x in data])

        if len(points) == 0:
            return []

        distances = np.sqrt(((points - centroids[:, np.newaxis]) ** 2).sum(axis = 2))
        min_distances = np.min(distances, axis = 0)

        for i, idx in enumerate(points_idx):
            yield (idx, min_distances[i])


    @staticmethod
    def initialize_centroids_non_uniform(points, k, sc):
        n_centroids = np.zeros(shape = (k, points.shape[1]))

        centroid = points[random.sample(range(points.shape[0]), 1)]
        n_centroids[0] = centroid

        rdd = sc.parallelize(points).zipWithIndex().cache()

        for i in range(k - 1):

            bc_centroids = sc.broadcast(n_centroids)

            data_closest_distances_rdd = rdd.mapPartitions(lambda x: FastKmeans.get_closest_distance(x, bc_centroids)).cache()

            sum_all = data_closest_distances_rdd.map(lambda x: (1, x[1])).reduceByKey(lambda x, y: x + y).collect()[0][1]

            sum_all *= np.random.random()

            distances = 0

            data_closest_distances = data_closest_distances_rdd.collect()
            for p in data_closest_distances:
                distances += p[1]

                if distances >= sum_all:
                    n_centroids[i + 1] = points[p[0]]
                    break

        return n_centroids


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
    def fit(data, k, redis_host = None, redis_port = 6379, sc = None, cluster_key = None):

        if not cluster_key:
            cluster_key = str(int(time.mktime(time.localtime())))

        retris = 0

        while True:
            n_centors = FastKmeans.initialize_centroids_non_uniform(data, k, sc)
            n_centors = n_centors[n_centors[:, 0].argsort()]

            centors = n_centors

            rdd = sc.parallelize(data)

            for i in range(FastKmeans.__iterations):
                #print("round %d ..."%i)

                broadcast_centors = sc.broadcast(n_centors)

                collect_data = rdd.mapPartitions(lambda x: FastKmeans.assign_to_new_centroids(x, broadcast_centors)).collect()

                n_centors, cluster_sizes = FastKmeans.move_to_new_centroids(collect_data)
                if n_centors is None:
                    break

                n_centors = n_centors[n_centors[:, 0].argsort()]

                if FastKmeans.center_diff(n_centors, centors) < FastKmeans.__converge_threshold:
                    break

                centors = n_centors

            if n_centors is not None:
                break

            retris += 1
            if retris >= FastKmeans.__MAX_RETRY_TIMES:
                return None

        #收敛后就把数据存储到redis cluster中
        if redis_host:
            result = rdd.mapPartitions(lambda x: FastKmeans.assign_to_final_centroids(x, n_centors)).groupByKey().\
                         map(lambda x: FastKmeans.save_cluster_to_redis(x[0], x[1], n_centors.shape[1], redis_host, redis_port, cluster_key)).collect()
        else:
            result = n_centors

        return result
