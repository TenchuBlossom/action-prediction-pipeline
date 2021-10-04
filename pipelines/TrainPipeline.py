import tools.file_system as fs
import tools.pipeline_tools as pt
import tools.py_tools as pyt
from tools.constants import Constants
cs = Constants()


class TrainPipeline:

    def __init__(self, data_config_src: str, trainable_config_src: str, analytic_config_src: str):

        self.data_config = fs.compile_config(data_config_src)
        self.trainable_config = fs.compile_config(trainable_config_src)
        self.analytic_config = None

        self.consumer = pt.compile_consumer(self.data_config)
        self.trainable = pt.compile_trainable(self.trainable_config)
        self.analytics = None

    def execute(self):
        self.consumer.consume()
        self.consumer.transform()
        x_train, x_test, y_train, y_test, feature_names = self.consumer.provide()

        self.trainable.train(x_train, y_train)
        self.trainable.evaluate(x_test, y_test)
        self.trainable.diagnose()


if __name__ == "__main__":

    d_src = '../configs/beta1_active/data.config.yaml'
    m_src = '../configs/beta1_active/trainable.config.yaml'
    a_src = ''

    pipe = TrainPipeline(d_src, m_src, a_src)
    pipe.execute()
    print(pipe.trainable.train_diagnostics['Descriptives']['MCC'])
