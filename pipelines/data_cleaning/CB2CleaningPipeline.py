import tools.file_system as fs
import tools.pipeline_tools as pt
import tools.py_tools as pyt
from tools.constants import Constants
cs = Constants()


class CB2CleaningPipeline:

    def __init__(self, data_config_src: str):

        self.data_config = fs.compile_config(data_config_src)
        self.consumer = pt.compile_consumer(self.data_config)

    def execute(self):
        while not self.consumer.processes_completed():
            self.consumer.consume()
            self.consumer.transform()


if __name__ == "__main__":

    d_src = '../../configs/cb2/data.config.yaml'

    pipe = CB2CleaningPipeline(d_src)
    pipe.execute()
