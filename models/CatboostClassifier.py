from catboost import CatBoostClassifier
from sklearn.base import BaseEstimator, ClassifierMixin


class Model(BaseEstimator, ClassifierMixin):

    def __init__(self, config):
        self.config = config
        self.base_estimator = CatBoostClassifier(**self.config)

    def fit(self, x, y=None):
        return self.base_estimator.fit(x, y)

    def predict(self, x):
        return self.base_estimator.predict(x)
