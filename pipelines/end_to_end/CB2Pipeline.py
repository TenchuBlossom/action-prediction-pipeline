import tools.file_system as fs
import tools.pipeline_tools as pt
import tools.py_tools as pyt
from tools.constants import Constants
cs = Constants()


class CB2Pipeline:

    def __init__(self, data_config_src: str, trainable_config_src: str, performance_profile=None):

        self.data_config = fs.compile_config(data_config_src)
        self.trainable_config = fs.compile_config(trainable_config_src)

        self.data_config = pyt.put(self.data_config, performance_profile, ['consumer', 'performance_profile'])
        self.data_config = pyt.put(self.data_config, performance_profile, ['provider', 'performance_profile'])

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

    d_src = '../../configs/cb2/data.config.yaml'
    t_src = '../../configs/cb2/trainable.config.yaml'

    pipe = CB2Pipeline(d_src, t_src, "sync")
    pipe.execute_downstream()

    a = 0
