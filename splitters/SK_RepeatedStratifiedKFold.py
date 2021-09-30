from sklearn.model_selection import RepeatedStratifiedKFold


class Splitter:

    def __init__(self, config):
        self.splitter = RepeatedStratifiedKFold(**config)
