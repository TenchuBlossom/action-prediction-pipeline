from sklearn.metrics import confusion_matrix, ConfusionMatrixDisplay
import tools.py_tools as pyt
import tools.file_system as fs
import numpy as np
import matplotlib.pyplot as plt
import seaborn as sns
import use_context
import tools.diagnostic_tools as dt


# Median Confusion Matrix
class Diagnostic:

    def __init__(self, config):
        self.config = config

    def __call__(self, results):

        with use_context.performance_profile("full-process"):
            plot = pyt.get(self.config, ['plot'])
            y_preds = pyt.get(results, ['y_preds'])
            y_true = pyt.get(results, ['y_true'])
            labels = pyt.get(self.config, ['labels'])
            label_names = [i[1] for i in labels.items()] if labels else None
            normalize = str(pyt.get(self.config, ['normalize'])).lower() if pyt.get(self.config, ['normalize']) else None

            if y_preds is None or y_true is None:
                raise KeyError(
                    f"Diagnostic {fs.get_class_filename(self)}: Input results is missing either y_preds or y_true array"
                )

            if len(y_preds) != len(y_true):
                raise ValueError(
                    f"Diagnostic {fs.get_class_filename(self)}: len of y_preds is not equal to len of y_true"
                )

            confusion_matricies = []
            for y_pred_fold, y_true_fold in zip(y_preds, y_true):
                confusion_matricies.append(confusion_matrix(y_pred_fold, y_true_fold, normalize=normalize))

            confusion_matricies = np.asarray(confusion_matricies)
            confusion_matricies = np.median(confusion_matricies, axis=0)

            if not plot or plot is None: return dict(confusion_matrix=confusion_matrix, figure=None)

            figure = dt.render_heatmap(confusion_matricies, x_label="True Label", y_label="Predicted Label", title="Confusion Matrix")
            return dict(confusion_matrix=confusion_matricies, figure=figure)


