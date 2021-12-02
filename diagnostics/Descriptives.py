import tools.py_tools as pyt
import numpy as np
import use_context
import tools.diagnostic_tools as dt
import pandas as pd

# Descriptives
class Diagnostic:

    def __init__(self, config):
        self.config = config

    def __call__(self, results):

        with use_context.performance_profile("full-process"):
            mean = pyt.get(self.config, ['mean'])
            median = pyt.get(self.config, ['median'])
            std = pyt.get(self.config, ['std'])
            ci = pyt.get(self.config, ['confidence_interval'])
            plot = pyt.get(self.config, ['plot'])

            descriptives = dict()
            for score_name, score_val in pyt.get(results, ['scores']).items():

                out = dict()
                if mean:
                    out['mean'] = np.mean(score_val)

                if median:
                    out['median'] = np.median(score_val)

                if std:
                    out['std'] = np.std(score_val)

                if ci:
                    # TODO normal confidence interval to be added
                    pass

                descriptives[score_name] = out
                figure = dt.render_mpl_table(pd.DataFrame.from_dict(descriptives),  header_columns=0, col_width=2.0)

            return dict(descriptives=descriptives, figure=figure)
