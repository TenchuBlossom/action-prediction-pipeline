import tools.py_tools as pyt
import tools.trainable_tools as tt
from sklearn.model_selection import KFold, cross_validate
from tools.constants import Constants
cs = Constants()


class Validator:

    def __init__(self, config):
        self.config = config

    # TODO Validators need to return the results of each fold, the model y_predictions and y_trues
    def __call__(self, model, x, y) -> dict:

        # TODO need to organise data into consistent format
        scores = cross_validate(model, x, y, **self.config)

        # if true then use estimators to also get predictons
        return_estimator = pyt.get(self.config, [cs.trainable, cs.validator, cs.parameters, 'return_estimator'])
        if not return_estimator:
            return dict(scores=scores, predictions=None)



