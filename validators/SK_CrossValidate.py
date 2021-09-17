import tools.py_tools as pyt
import tools.trainable_tools as tt
from sklearn.model_selection import KFold, cross_validate
from tools.constants import Constants
cs = Constants()


class Validator:

    def __init__(self, config):
        self.config = config

    def __call__(self, model, x, y) -> dict:
        return cross_validate(model, x, y, **self.config)
