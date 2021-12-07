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

        used_model = results['used_model'].base_estimator
        used_x_data = results['used_x_data']
        features = results['used_features']
        shap_vales = shap.TreeExplainer(used_model).shap_values(used_x_data)

        sns.set()

        plt.figure(0)
        shap.summary_plot(
            shap_values=shap_vales,
            features=used_x_data,
            feature_names=features,
            show=False,
            plot_type='dot',
            max_display=20
        )
        plt.tight_layout()
        figure1 = plt.gcf()

        plt.figure(1)
        shap.summary_plot(
            shap_values=shap_vales,
            features=used_x_data,
            plot_type='bar',
            feature_names=features,
            show=False,
            max_display=20
        )
        plt.tight_layout()
        figure2 = plt.gcf()

        return dict(shaps=shap_vales, figures={'Shaps-Local': figure1, 'Shaps-global': figure2})
