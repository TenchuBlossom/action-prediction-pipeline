from sklearn.metrics import matthews_corrcoef, make_scorer


class Scorer:

    def __init__(self):
        self.scorer = lambda y, y_preds: matthews_corrcoef(y, y_preds)
        self.compiled_scorer = make_scorer(self.scorer)

