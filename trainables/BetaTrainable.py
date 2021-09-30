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
        self.evaluation_results = None

        self.train_diagnostics = None
        self.eval_diagnostics = None

    def train(self, x, y):
        self.train_results = self.validator(self.model, x, y)

    def evaluate(self, x, y):
        pass

    def diagnose(self):

        if self.train_results is None and self.evaluation_results is None:
            print()
            return

        if self.train_results is not None:
            self.train_diagnostics = tt.execute_sync_diagnostics(self.train_results, self.diagnostic_chain)
            print()

        if self.evaluation_results is not None:
            self.eval_diagnostics = tt.execute_sync_diagnostics(self.evaluation_results, self.diagnostic_chain)
            print()







