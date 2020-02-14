import random
import sys
import logging
from feature import Feature
from redis import ConnectionPool, Redis

logger = logging.getLogger('PyCurtain. ')
logger.setLevel(logging.INFO)
logger.addHandler(logging.StreamHandler(sys.stdout))

class PyCurtain():

    def __init__(self, host='localhost', port=6379, db=0):
        self.redis_pool = ConnectionPool(host=host, port=port, db=db)

    def is_open(self, feature, user=None):
        try:
            if user is not None:
                return self.__is_open_for_user(feature, user) or self.__is_feature_open(feature)
            else:
                return self.__is_feature_open(feature)
        except Exception as e:
            logger.exception(
                f"[PyCurtain] Redis error. Returning default value FALSE for [feature={feature}]", e)
            return False

    def open_feature_for_user(self, feature, user):
        try:
            redis = Redis(connection_pool=self.redis_pool)
            redis.sadd(f"feature:{feature}:users", user)
        except Exception as e:
            logger.exception(
                f"[PyCurtain] Redis error. Returning default value FALSE for [user={user},feature={feature}]", e)

    def get_feature(self, name):
        try:
            redis = Redis(connection_pool=self.redis_pool)
            return Feature(name,self.__get_feature_percentage(name), redis.smembers(f"feature:{name}:users"))
        except Exception as e:
            logger.exception(
                f"[PyCurtain] Redis error while getting data from feature [{name}]", e)

    def __is_feature_open(self, feature):
        return self.__random_percentage() <= self.__get_feature_percentage(feature)

    def __is_open_for_user(self, feature, user):
        try:
            redis = Redis(connection_pool=self.redis_pool)
            return redis.sismember("feature:" + feature + ":users", user)
        except:
            pass

    def __random_percentage(self):
        return random.randint(0, 99) + 1

    def __get_feature_percentage(self, feature):
        try:
            redis = Redis(connection_pool=self.redis_pool)
            feature_percentage = redis.get(f"feature:{feature}:percentage")
            return 0 if feature_percentage is None else int.from_bytes(feature_percentage)
        except:
            pass
