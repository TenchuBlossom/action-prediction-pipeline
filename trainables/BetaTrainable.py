from tools.constants import Constants
import tools.trainable_tools as tt
import tools.py_tools as pyt
import tools.file_system as fs
from alive_progress import alive_bar
import os
cs = Constants()


class Trainable:

    def __init__(self, config=None):

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
        with alive_bar(title="Executing Training Procedure", bar='classic') as bar:
            self.train_results = self.validator(self.model, x, y)
            bar()

    def evaluate(self, x, y):
        pass

    def diagnose(self, location, sync=True):

        if self.train_results is None and self.evaluation_results is None:
            print()
            return

        if self.train_results is not None:
            txt = 'Applying training diagnostics'
            self.train_diagnostics = tt.execute_sync_diagnostics(self.train_results, self.diagnostic_chain, txt)
            tt.persist_diagnostics(location, self.train_diagnostics)

        if self.evaluation_results is not None:
            txt = 'Applying evaluation diagnostics'
            self.eval_diagnostics = tt.execute_sync_diagnostics(self.evaluation_results, self.diagnostic_chain, txt)
            tt.persist_diagnostics(location, self.eval_diagnostics)

    def persist(self, location: str):
        trainable_name = pyt.get(self.config, ['name'], 'trainable')
        fs.save_python_entity(fs.path(os.path.join(location, f'{trainable_name}_trainable.tr')), self)








