from catboost import CatBoostClassifier


class Model:

    def __init__(self, parameters: dict):

        self.parameters = parameters
        self.model = None

    def init_parameters(self):
        self.model = CatBoostClassifier(**self.parameters)

    def fit(self, x, y, eval_x=None, eval_y=None):

        if eval_x is None or eval_y is None:
            self.model.fit(x, y)
            return

        self.model.fit(x, y, eval_set=(eval_x, eval_y))

    def predict(self, x):
        return self.model.predict(x, prediction_type='Class')

    def predict_proba(self, x):
        return self.model.predict(x)
