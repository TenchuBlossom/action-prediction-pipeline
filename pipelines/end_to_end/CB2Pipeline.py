import tools.file_system as fs
import tools.pipeline_tools as pt
from multiprocessing import freeze_support
from tools.performance_profile_tools import PerformanceProfile
import tools.file_system as fs
from tools.constants import Constants
import tools.py_tools as pyt
from tqdm import tqdm
import ray
import use_context
import os
cs = Constants()


class CB2Pipeline:

    def __init__(self, config_src: str):

        self.config = fs.compile_config(config_src)
        self.data_config = fs.compile_config(self.config['pipeline']['data_config'])
        self.trainable_config = fs.compile_config(self.config['pipeline']['train_config'])

        self.consumer = pt.compile_consumer(self.data_config)
        self.provider = pt.compile_provider(self.data_config)
        self.trainable = pt.compile_trainable(self.trainable_config)

        use_context.performance_profile = PerformanceProfile(self.config['performance_profile'])

        self.pipe_location = fs.make_dir_chain(
            fs.path('../../resources'),
            ['pipelines', pyt.get(self.config, ['pipeline', 'name'], 'pipeline')]
        )

    def execute_procedures(self, procedure):
        procedure_list = pyt.get(self.config, ['procedures', procedure], [])

        for proc_key in procedure_list:
            procedure_method = getattr(self, proc_key)
            procedure_method()

        self.procedure_shutdown()

    def procedure_preprocess(self):

        self.consumer.spin_up_processes()
        desc = f"CB2 Pipeline: Cleaning Batches of size {self.consumer.chunksize}"
        with tqdm(total=self.consumer.total_length, desc=desc) as pbar:
            while not self.consumer.processes_completed():

                no_of_samples = self.consumer.consume()
                if self.consumer.processes_completed():
                    continue
                self.consumer.transform()
                pbar.update(no_of_samples)

        print('PIPELINE COMPLETE: Beginning shutdown process =>')
        self.consumer.spin_down_processes()
        use_context.performance_profile.close()

    def procedure_train(self):
        x_train, x_test, y_train, y_test, features = self.provider.provide()

        self.trainable.train(x_train, y_train)
        self.trainable.evaluate(x_test, y_test)

    def procedure_fit(self):
        x_all, _, y_all, _, features = self.provider.provide()
        self.trainable.fit(x_all, y_all)

    def procedure_diagnose(self):
        self.trainable.diagnose(self.pipe_location)

    def procedure_persist(self):

        if pyt.get(self.config, ['save_options', 'save_trainable'], False):
            self.trainable.persist(location=self.pipe_location)

    def procedure_shutdown(self):
        use_context.performance_profile.close()


def entry_point(config: str, procedure: str):
    ray.init(log_to_driver=False)
    pipe = CB2Pipeline(config)
    pipe.execute_procedures(procedure)
    # trainable = fs.load_class_instance(fs.path('../../resources/pipelines/CB2Pipeline.pipe'), uncompress=False)
    # trainable.diagnose()
    ray.shutdown()


def parse_arguments():
    # arg parser goes here
    pass


if __name__ == "__main__":
    freeze_support()
    parse_arguments()
    entry_point('../../configs/cb2_DT/pipeline.config.yaml', 'procs_1')