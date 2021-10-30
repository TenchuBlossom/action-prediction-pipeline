import tools.file_system as fs
import tools.pipeline_tools as pt
import tools.py_tools as pyt
from tools.constants import Constants
cs = Constants()


class CB2Pipeline:

    def __init__(self, config_src: str):

        self.config = fs.compile_config(config_src)
        self.data_config = pt.compile_data_config(self.config, self.config['pipeline']['data_config'])
        self.trainable_config = pt.compile_train_config(self.config, self.config['pipeline']['train_config'])

        self.consumer = pt.compile_consumer(self.data_config)
        self.provider = pt.compile_provider(self.data_config)
        self.trainable = pt.compile_trainable(self.trainable_config)

    def execute_clean(self):
        while not self.consumer.processes_completed():
            self.consumer.consume()
            if self.consumer.processes_completed():
                continue
            self.consumer.transform()

    def execute_downstream(self):
        x_train, x_test, y_train, y_test, features = self.provider.provide()

        self.trainable.train(x_train, y_train)
        self.trainable.evaluate(x_test, y_test)
        self.trainable.diagnose()

        a = 0


if __name__ == "__main__":

    pipe = CB2Pipeline('../../configs/cb2/pipeline.config.yaml')
    pipe.execute_clean()

    a = 0
