import tools.py_tools as pyt
import tools.trainable_tools as tt
from sklearn.model_selection import KFold, cross_validate
from tools.constants import Constants
import use_context
cs = Constants()


class Validator:

    def __init__(self, config):
        self.config = config

    # TODO Validators need to return the results of each fold, the model y_predictions and y_trues
    def __call__(self, model, scorers, x, y) -> dict:

        # TODO need to organise data into consistent format
        with use_context.performance_profile("cross-validation"):
            scores = cross_validate(model, x, y, scoring=scorers, **self.config)

        with use_context.performance_profile("validator-post-processing"):
            scorer_names = list(scores.keys())
            results = dict(scores=dict(), y_preds=[], y_true=[])

            # extract the score results from sklearn
            for key, value in scores.items():
                for scorer_name in scorer_names:

                    if key.endswith(scorer_name):
                        results = pyt.put(results, value, key_chain=['scores', scorer_name])

            # if true then use estimators to also get predictons
            return_estimator = pyt.get(self.config, ['return_estimator'])
            if not return_estimator: return results

            for model in scores['estimator']:
                results['y_preds'].append(model.y_preds)
                results['y_true'].append(model.y_true)

            return results





