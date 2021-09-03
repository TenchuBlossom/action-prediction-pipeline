import tools.file_system as fs
import tools.pipeline_tools as pt


class TrainPipeline:

    def __init__(self, data_config_src: str, model_config_src: str, analytic_config_src: str):

        self.data_config = fs.compile_config(data_config_src)
        self.model_config = None
        self.analytic_config = None

        self.consumer = pt.compile_consumer(self.data_config)
        self.model = None
        self.analytics = None

    def execute(self):
        pass


if __name__ == "__main__":

    pipe = TrainPipeline('../configs/beta1_active/data.config.yaml', '', '')
    processed_data = pipe.consumer.provide()
    a = 0