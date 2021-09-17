from sklearn.model_selection import KFold
from tools.constants import Constants
cs = Constants()


class Splitter:

    def __init__(self, config):
        self.splitter = KFold(**config)
