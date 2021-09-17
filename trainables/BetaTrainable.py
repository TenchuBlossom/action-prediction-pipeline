from tools.constants import Constants
import tools.trainable_tools as tt
cs = Constants()


class Trainable:

    def __init__(self, config: dict):

        self.config = config
        self.splitter = tt.compile_splitter(config)
        self.model = tt.compile_model(config)
        self.validator = tt.compile_validator(config, self.splitter)
        self.results = None

    def fit(self, x, y):
        results = self.validator(self.model, x, y)
        self.results = results
        return results


