from sklearn.metrics import confusion_matrix


# Median Confusion Matrix
class Diagnostic:

    def __call__(self, results, labels):
        a = 0