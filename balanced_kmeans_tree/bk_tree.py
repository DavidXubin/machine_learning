import redis
import random
import numpy as np
from .redis_db import RedisDBWrapper
from .fast_kmeans import FastKmeans


#代表Balanced K-means tree的节点
class BKTNode(object):

    def __init__(self):
        self.id = str(id(self))
        self.cluster_id = None
        self.children = []
        self.parent = None
        self.centriod = None
        self.leaf = False

    #读取该节点的数据，如果是叶节点，则只读取它所对应的簇数据，如果是非叶节点，则读取它子树的所有簇数据
    #参数 redis_handler:  redis handler
    #参数 bkt_key:        该节点所在的树的唯一标识
    def get_data(self, redis_handler, bkt_key):
        if not redis_handler:
            return None

        if self.leaf:
            return redis_handler.get_data(bkt_key + '_' + self.parent.id + '_' + str(self.cluster_id))

        merged_data = np.zeros(shape = (1, self.centriod.shape[0]))

        for child in self.children:
            data = child.get_data(redis_handler, bkt_key)
            merged_data = np.concatenate((merged_data, data), axis = 0)

        return merged_data[1:]


#代表Balanced K-means tree
class BKTree(object):

    #初始化Balanced K-means tree
    #参数 max_clusters_per_run: 指定每次K-means聚类的最大簇数量
    #参数 max_depth           : 指定树的高度限制
    #参数 min_cluster_size    : 指定簇中数据的最小数量，即最小簇大小
    #参数 sc                  : spark context
    #参数 redis_host          : 指定用于存储簇数据的redis host
    #参数 redis_port          : 指定用于存储簇数据的redis port
    #参数 max_cluster_size    : 指定簇中数据的最大数量，是个软约束，如果不指定的话就默认是min_cluster_size的2倍
    def __init__(self, max_clusters_per_run, max_depth, min_cluster_size, sc, redis_host, redis_port = 6379, max_cluster_size = 0):
        self.__max_clusters_per_run = max_clusters_per_run
        self.__max_depth = max_depth
        self.__min_cluster_size = min_cluster_size
        self.__max_cluster_size = max(self.__min_cluster_size, max_cluster_size)
        self.__sc = sc
        self.__redis_host = redis_host
        self.__redis_port = redis_port
        self.__bkt_key = str(id(self))
        self.__root_node = BKTNode()

        self.__redis = RedisDBWrapper(redis_host, redis_port)

    def get_root(self):
        return self.__root_node

    def get_key(self):
        return self.__bkt_key

    def get_redis(self):
        return self.__redis.getHandler()

    def dump(self):

        epochTime = int(time.mktime(time.localtime()))

        tree_params = self.__bkt_key + '_' + str(self.__max_clusters_per_run) + '_' + str(self.__max_depth) + '_' \
                      + str(self.__min_cluster_size) + '_' + str(self.__max_cluster_size) + '_' + self.__root_node.id

        self.get_redis().sadd('my_bkt', str(epochTime) + '_' + tree_params)

        q = queue.Queue()
        q.put(self.__root_node)

        try:
            while not q.empty():
                node = q.get()

                if node.leaf:
                    value = '1_' + str(node.cluster_id)
                elif node.cluster_id:
                    value = '0_' + str(node.cluster_id)
                else:
                    value = '0_0'

                ret = self.get_redis().set(self.__bkt_key + '_' + node.id, value)
                if not ret:
                    raise Exception("Failed to dump BKT node to redis")

                for child in node.children:
                    ret = self.get_redis().sadd(self.__bkt_key + '_' + node.id + '_children', child.id)
                    if not ret:
                        raise Exception("Failed to dump BKT sub tree to redis")

                    q.put(child)

            return True
        except Exception as e:
            print(e)
            return False


    def update_centroid(self, node):

        if node.leaf:
            data = self.__redis.get_data(bkt.__bkt_key + '_' + node.parent.id + '_' + str(node.cluster_id))
            total_sum = data.sum(axis = 0)
            total_size = len(data)

            node.centriod = total_sum / total_size

            return (total_sum, total_size)

        cluster_list = []
        for child in node.children:
            cluster_list.append(self.update_centroid(child))

        total_sum = np.zeros(shape = (cluster_list[0][0].shape[0], ))
        total_size = 0

        for sum, size in cluster_list:
            total_sum += sum
            total_size += size

        node.centriod = total_sum / total_size

        return (total_sum, total_size)


    @classmethod
    def loads(cls, bkt_key, redis_host, redis_port = 6379):

        redis = RedisDBWrapper(redis_host, redis_port)

        trees = redis.getHandler().smembers('my_bkt')
        found = False

        for tree in trees:
            if bkt_key == tree.split('_')[1]:

                _, bkt_key, max_clusters_per_run, \
                max_depth, min_cluster_size, max_cluster_size, root_id = tree.split('_')

                found = True
                break

        if not found:
            return None

        bkt = BKTree(int(max_clusters_per_run), int(max_depth), int(min_cluster_size), \
                     None, redis_host, redis_port, int(max_cluster_size))

        bkt.__bkt_key = bkt_key
        bkt.__redis = redis
        bkt.__root_node.id = root_id

        try:
            q = queue.Queue()
            q.put(bkt.__root_node)

            while not q.empty():
                node = q.get()

                if not node.leaf:
                    children_ids = bkt.get_redis().smembers(bkt.__bkt_key + '_' + node.id + '_children')
                else:
                    data = bkt.__redis.get_data(bkt.__bkt_key + '_' + node.parent.id + '_' + str(node.cluster_id))

                    node.centriod = data.sum(axis = 0) / len(data)
                    children_ids = []


                for child_id in children_ids:
                    child_info = bkt.get_redis().get(bkt.__bkt_key + '_' + child_id)

                    child_node = BKTNode()
                    child_node.id = child_id

                    child_node.leaf = True if child_info.split('_')[0] == '1' else False
                    child_node.cluster_id = int(child_info.split('_')[1])
                    child_node.parent = node

                    q.put(child_node)

                    node.children.append(child_node)

            bkt.update_centroid(bkt.__root_node)

            return bkt

        except Exception as e:
            print(e)
            return None



    def __get_nearest_cluster(self, point, node, leaf_clusters):

        if node.leaf:
            leaf_clusters.append((node, np.sqrt(((point - node.centriod) ** 2).sum())))

        for child in node.children:
            self.__get_nearest_cluster(point, child, leaf_clusters)

    #返回与指定的数据点最接近的簇所对应的节点
    #参数 point: 要检索的数据点
    def get_nearest_leaf_node(self, point):

        leaf_clusters = []

        for child in self.__root_node.children:
            self.__get_nearest_cluster(point, child, leaf_clusters)

        if len(leaf_clusters) == 0:
            return None

        leaf_clusters = sorted(leaf_clusters, key = lambda x: x[1])

        return leaf_clusters[0][0]

    #获取与指定的数据点最接近的簇数据
    #参数 point: 要检索的数据点
    def get_nearest_cluster(self, point):

        node = self.get_nearest_leaf_node(point)
        if node:
            return node.get_data(self.__redis, self.__bkt_key)

        return None

    #构建balanced k-means tree
    #参数 data:  numpy array类型的数据集
    def build(self, data):
        size = len(data)
        if size == 0:
            return self.__root_node

        max_cluster_size = max(self.__min_cluster_size * 2, self.__max_cluster_size)

        if size <= max_cluster_size:
            cur_node = BKTNode()
            cur_node.parent = self.__root_node
            cur_node.centriod = data.sum(axis = 0) / len(data)
            cur_node.cluster_id = 0

            self.__redis.save_data(data, self.__bkt_key + '_' + self.__root_node.id + "_" + str(cur_node.cluster_id))
            self.__root_node.children.append(cur_node)

            return self.__root_node

        k = min(size / max_cluster_size + 1, self.__max_clusters_per_run)

        clusters = FastKmeans.fit(data, k, self.__redis_host, self.__redis_port, self.__sc, self.__bkt_key + '_' + self.__root_node.id)

        for cid, centriod, _, ret in clusters:
            if not ret:
                print("Failed to build BKT due to redis write error")
                return None

            child_node = self.make_bkt_node(cid, centriod, self.__root_node)
            if not child_node:
                print("Failed to build BKT")
                return None

            self.__root_node.children.append(child_node)

        self.adjust_tree_depth()

        return self.__root_node

    #构建balanced k-means tree的节点
    #参数 centriod_id： 该节点所对应的簇id
    #参数 centriod：    该节点所对应的簇中心
    #参数 parent_node： 该节点的父节点
    def make_bkt_node(self, cluster_id, centriod, parent_node):

        cur_node = BKTNode()
        cur_node.parent = parent_node
        cur_node.centriod = centriod
        cur_node.cluster_id = cluster_id

        #读取该节点所对应簇的数据
        data = self.__redis.get_data(self.__bkt_key + '_' + parent_node.id + '_' + str(cluster_id))

        size = len(data)
        max_cluster_size = max(self.__min_cluster_size * 2, self.__max_cluster_size)

        #如果该节点所对应簇的数据量小于max_cluster_size，则完成一个叶子节点
        if size <= max_cluster_size:
            cur_node.leaf = True
            return cur_node

        #计算该节点中的数据还可以分成几个簇，如果只能分成2个簇，则做均分处理，否则进行一轮K-means聚类
        k = min(int(size / max_cluster_size + 1), self.__max_clusters_per_run)
        if k == 2:
            clusters = self.half_cut_cluster(data, self.__bkt_key + '_' + cur_node.id)
        else:
            clusters = FastKmeans.fit(data, k, self.__redis_host, self.__redis_port, self.__sc, self.__bkt_key + '_' + cur_node.id)

        if False in [ret for _, _, _, ret in clusters]:
            print("Failed to build BKT due to redis write error")
            return None

        #检查K-means聚类所产生的簇，将小于min_cluster_size的簇归并起来
        if k > 2:
            clusters = self.merge_small_clusters(clusters, cur_node)

        #对聚类所形成的新簇递归构建子树
        for cid, centriod, _, _ in clusters:
            child_node = self.make_bkt_node(cid, centriod, cur_node)
            if not child_node:
                return None

            cur_node.children.append(child_node)

        return cur_node

    #将数据切分成大小相等的两个簇
    def half_cut_cluster(self, data, cluster_key, cluster_ids = ()):

        clusters = []
        if len(cluster_ids) == 0:
            first_id = 0
            second_id = 1
        else:
            first_id, second_id = cluster_ids

        data_len = int(len(data) / 2)

        ret = self.__redis.save_data(data[:data_len], cluster_key + "_" + str(first_id))
        cluster = (first_id, data[:data_len].sum(axis = 0) / data_len, data_len, ret)
        clusters.append(cluster)

        data_len = len(data) - int(len(data) / 2)
        ret = self.__redis.save_data(data[int(len(data) / 2):], cluster_key + "_" + str(second_id))
        cluster = (second_id, data[int(len(data) / 2):].sum(axis = 0) / data_len, data_len, ret)
        clusters.append(cluster)

        return clusters

    #检查K-means聚类所产生的簇，将小于min_cluster_size的簇归并起来
    def merge_small_clusters(self, clusters, parent_node):

        clusters = sorted(clusters, key = lambda x: x[2])
        to_merges = []

        for i, cluster in enumerate(clusters):
            if cluster[2] < self.__min_cluster_size:
                to_merges.append((cluster[0], cluster[2]))
            else:
                clusters = clusters[i:]
                break

        max_cluster_size = max(self.__min_cluster_size * 2, self.__max_cluster_size)

        if len(to_merges) == 0:
            return clusters


        if len(to_merges) == len(clusters):
            clusters = []

        i = 0
        new_cid = max([cluster[0] for cluster in clusters]) + 1
        new_clusters = []

        while i < len(to_merges):

            merged_data = np.zeros(shape = (1, parent_node.centriod.shape[0]))

            size = 0

            while i < len(to_merges) and size + to_merges[i][1] <= max_cluster_size:

                data = self.__redis.get_data(self.__bkt_key + '_' + parent_node.id + '_' + str(to_merges[i][0]))

                merged_data = np.concatenate((merged_data, data), axis = 0)

                size += to_merges[i][1]

                i += 1

            if size >= self.__min_cluster_size and size <= max_cluster_size:

                merged_data = merged_data[1:]
                ret = self.__redis.save_data(merged_data, self.__bkt_key + '_' + parent_node.id + '_' + str(new_cid))

                new_cluster = (new_cid, merged_data.sum(axis = 0) / len(merged_data), len(merged_data), ret)
                new_clusters.append(new_cluster)

            #如果就剩下最后一个小簇，那么表示在它前面的小簇都合并完了，并且合并后的大小小于max_cluster_size，那么就把这最后一个小簇也合并掉，
            #这样这个簇大小肯定超过max_cluster_size了，可以继续在下次迭代中分裂。
            if i == len(to_merges) - 1:

                data = self.__redis.get_data(self.__bkt_key + '_' + parent_node.id + '_' + str(to_merges[i][0]))

                merged_data = np.concatenate((merged_data, data), axis = 0)
                ret = self.__redis.save_data(merged_data, \
                                             self.__bkt_key + '_' + parent_node.id + '_' + str(new_cid))

                new_cluster = (new_cid, merged_data.sum(axis = 0) / len(merged_data), len(merged_data), ret)
                new_clusters[len(new_clusters) - 1] = new_cluster
                break

            #如果小簇合并后还是小于min_cluster_size，那么就和第一个大簇合并，并且将合并之后的簇再对半切成两个均等的簇，继续在下次迭代中分裂。
            #在这一步中，如果不做对半切，算法可能永远无法收敛
            if size < self.__min_cluster_size and len(clusters) > 0:

                data = self.__redis.get_data(self.__bkt_key + '_' + parent_node.id + '_' + str(clusters[0][0]))
                merged_data = np.concatenate((merged_data, data), axis = 0)

                new_clusters.extend(self.half_cut_cluster(merged_data[1:], self.__bkt_key + '_' + parent_node.id, (new_cid, new_cid + 1)))
                new_cid += 1
                clusters = clusters[1:]
                break

            new_cid += 1

        new_clusters.extend(clusters)

        return new_clusters

    #获取指定节点的子树高度
    def get_tree_depth(self, node):

        if len(node.children) == 0:
            return 0

        depths = []
        for child in node.children:
            depths.append(self.get_tree_depth(child) + 1)

        return max(depths)

    #调整指定节点下的子树高度
    def __adjust_tree_depth(self, node):

        if len(node.children) == 0:
            return

        all_leaf_childs = np.array([child.leaf for child in node.children]).all()

        if not all_leaf_childs:
            children = node.children

            for child in children:
                if not child.leaf:
                    self.__adjust_tree_depth(child)

            if len(children) != len(node.children):
                merged_data = np.zeros(shape = (1, node.centriod.shape[0]))

                for child in node.children:
                    data = self.__redis.get_data(self.__bkt_key + '_' + node.id + '_' + str(child.cluster_id))

                    merged_data = np.concatenate((merged_data, data), axis = 0)

                node.centriod = merged_data.sum(axis = 0) / (len(merged_data) - 1)

            return

        parent_cluster_ids = [child.cluster_id for child in node.parent.children]
        new_cluster_id = max(parent_cluster_ids) + 1

        for child in node.children:
            data = self.__redis.get_data(self.__bkt_key + '_' + node.id + '_' + str(child.cluster_id))
            if data is None:
                print(self.__bkt_key + '_' + node.id + '_' + str(child.cluster_id))

            self.__redis.save_data(data, self.__bkt_key + '_' + node.parent.id + '_' + str(new_cluster_id))

            child.cluster_id = new_cluster_id
            child.parent = node.parent

            node.parent.children.append(child)

            new_cluster_id += 1

        node.parent.children.remove(node)

    #调整树高度
    def adjust_tree_depth(self):

        while self.get_tree_depth(self.__root_node) > self.__max_depth:
            self.__adjust_tree_depth(self.__root_node)
