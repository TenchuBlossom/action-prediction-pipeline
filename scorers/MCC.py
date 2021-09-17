from sklearn.metrics import matthews_corrcoef, make_scorer


class Scorer:

    def __init__(self):
        self.scorer = make_scorer(lambda y, y_preds: matthews_corrcoef(y, y_preds))

