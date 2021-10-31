from catboost import CatBoostClassifier
from sklearn.base import BaseEstimator, ClassifierMixin
import use_context


class Model(BaseEstimator, ClassifierMixin):

    def __init__(self, config):
        self.config = config
        self.base_estimator = CatBoostClassifier(**self.config) if config else CatBoostClassifier()

        self.y_true = None
        self.y_preds = None

    def fit(self, x, y=None):
        with use_context.performance_profile("model-fit", "fold"):
            self.base_estimator.fit(x, y)
            self.y_preds = self.predict(x)
            self.y_true = y
            return self

    def predict(self, x):
        with use_context.performance_profile("model-predict", "fold"):
            return self.base_estimator.predict(x)
