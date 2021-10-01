from sklearn.base import BaseEstimator, ClassifierMixin
from sklearn.ensemble import RandomForestClassifier


class Model(BaseEstimator, ClassifierMixin):

    def __init__(self, config):
        self.config = config
        self.base_estimator = RandomForestClassifier(**self.config)

        self.y_true = None
        self.y_preds = None

    def fit(self, x, y=None):
        self.base_estimator.fit(x, y)
        self.y_preds = self.predict(x)
        self.y_true = y
        return self

    def predict(self, x):
        return self.base_estimator.predict(x)
