import tools.file_system as fs
import tools.pipeline_tools as pt


class TrainPipeline:

    def __init__(self, data_config_src: str, model_config_src: str, analytic_config_src: str):

        self.data_config = fs.compile_config(data_config_src)
        self.model_config = fs.compile_config(model_config_src)
        self.analytic_config = None

        self.consumer = pt.compile_consumer(self.data_config)
        self.model = pt.compile_model(self.model_config)
        self.analytics = None

    def execute(self):
        pass


if __name__ == "__main__":

    d_src = '../configs/beta1_active/data.config.yaml'
    m_src = '../configs/beta1_active/model.config.yaml'
    a_src = ''

    pipe = TrainPipeline(d_src, m_src, a_src)
    pipe.consumer.consume()
    pipe.consumer.transform()
    x_train, x_test, y_train, y_test, feature_names = pipe.consumer.provide()

    pipe.model.init_parameters()
    pipe.model.fit(x_train, y_train)
    out = pipe.model.predict(x_train)
    a = 0