import redis
import struct
import numpy as np

class RedisDBWrapper(object):

    CHUNK_SIZE = 100000
    #Max redis string value is 512MB
    MAX_CHUNK_SIZE = 500000000

    def __init__(self, host, port = 6379):
        pool = redis.ConnectionPool(host = host, port = port, decode_responses = True)
        self.__redis = redis.Redis(connection_pool = pool)

    def getHandler(self):
        return self.__redis

    def __save_chunk_data(self, data, key, index):
        """Store given Numpy array 'a' in Redis under key 'n'"""
        try:
            h, w = data.shape
            shape = struct.pack('>II', h, w)
            encoded = shape + data.tobytes()

            self.__redis.set(key + "_" + str(index), encoded.decode('latin1'))
            return True
        except Exception as e:
            print(e)
            return False

    def __save_head_chunk_data(self, data, key, total_w, total_h, chunk_num):

        try:
            h, w = data.shape
            head = struct.pack('>III', total_w, total_h, chunk_num)
            shape = struct.pack('>II', h, w)
            encoded = head + shape + data.tobytes()

            self.__redis.set(key + "_0", encoded.decode('latin1'))
            return True
        except Exception as e:
            print(e)
            return False


    def __get_chunk_data(self, key, index):
        """Retrieve Numpy array from Redis key 'n'"""
        try:
            encoded = self.__redis.get(key + "_" + str(index))
            if not encoded:
                return None
            encoded = encoded.encode('latin1')

            h, w = struct.unpack('>II', encoded[:8])
            return np.frombuffer(encoded, offset = 8).reshape(h,w)
        except Exception as e:
            print(e)
            return None

    def __get_head_chunk_data(self, key):

        try:
            encoded = self.__redis.get(key + "_0")
            if not encoded:
                return 0, 0, 0, None
            encoded = encoded.encode('latin1')

            total_h, total_w, chunk_num = struct.unpack('>III', encoded[:12])
            h, w = struct.unpack('>II', encoded[12 : 20])
            return total_h, total_w, chunk_num, np.frombuffer(encoded, offset = 20).reshape(h,w)
        except Exception as e:
            print(e)
            return 0, 0, 0, None

    def save_data(self, data, key):

        size = 0
        index = 0

        if len(data) <= RedisDBWrapper.CHUNK_SIZE:
            chunk_num = 1
        elif len(data) % RedisDBWrapper.CHUNK_SIZE == 0:
            chunk_num = int(len(data) / RedisDBWrapper.CHUNK_SIZE)
        else:
            chunk_num = int(len(data) / RedisDBWrapper.CHUNK_SIZE) + 1

        while size < len(data):

            offset = min(RedisDBWrapper.CHUNK_SIZE, len(data) - size)

            firstChunk = False
            if size == 0:
                firstChunk = True

            if index == 0:
                ret = self.__save_head_chunk_data(data[size : size + offset], key, data.shape[0], data.shape[1], chunk_num)
            else:
                ret = self.__save_chunk_data(data[size : size + offset], key, index)

            if not ret:
                return False

            size += offset

            index += 1

        return True


    def get_data(self, key):

        total_h, total_w, chunk_num, head_chunk_data = self.__get_head_chunk_data(key)
        if total_h == 0:
            return None

        for index in range(chunk_num - 1):
            chunk_data = self.__get_chunk_data(key, index + 1)

            head_chunk_data = np.concatenate((head_chunk_data, chunk_data), axis = 0)

        head_chunk_data.reshape(total_h, total_w)
        return head_chunk_data
