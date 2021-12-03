import tools.py_tools as pyt
import numpy as np
import use_context
import tools.diagnostic_tools as dt
import pandas as pd
import seaborn as sns
import shap
import matplotlib.pyplot as plt


class Diagnostic:

    def __init__(self, config):
        self.config = config

    def __call__(self, results):

        with use_context.performance_profile("full-process"):
            used_model = results['used_model'].base_estimator
            used_x_data = results['used_x_data']
            shap_vales = shap.TreeExplainer(used_model).shap_values(used_x_data)
            a = 0

            # sns.set()
            # shap.summary_plot(
            #     shap_values=shap_vales,
            #     features=used_x_data,
            #     feature_names=self.data['feature_names'],
            #     show=False,
            #     plot_type='dot',
            #     max_display=10
            # )
            # plt.tight_layout()
            # figure1 = plt.gcf()
            # plt.clf()
            #
            # shap.summary_plot(
            #     shap_vales,
            #     x_data,
            #     plot_type='bar',
            #     feature_names=self.data['feature_names'],
            #     show=False,
            #     max_display=10
            # )

            # return dict(descriptives=descriptives, figure=figure)
