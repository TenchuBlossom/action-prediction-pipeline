from tools.constants import Constants
import tools.trainable_tools as tt
cs = Constants()


class Trainable:

    def __init__(self, config: dict):

        self.config = config
        self.splitter = tt.compile_splitter(config)
        self.model = tt.compile_model(config)
        self.validator = tt.compile_validator(config, self.splitter)
        self.diagnostic_chain = tt.compile_diagnostics(config)

        self.train_results = None
        self.predict_results = None
        self.test_results = None

    def train(self, x, y):
        results = self.validator(self.model, x, y)
        self.train_results = results
        return results

    def predict(self, x, y):
        pass

    def diagnose(self):

        for diagnostic in self.diagnostic_chain:
            pass




