import tools.file_system as fs
import tools.pipeline_tools as pt
import tools.py_tools as pyt
from multiprocessing import freeze_support
from tools.performance_profile_tools import PerformanceProfile
from tools.constants import Constants
from tqdm import tqdm
import ray
from ray.util.multiprocessing import Pool
import use_context
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

    def execute_clean(self):

        self.consumer.spin_up_processes()
        desc = f"CB2 Pipeline: Cleaning Batches of size {self.consumer.chunksize}"
        prog = 0
        with tqdm(total=self.consumer.total_length, desc=desc) as pbar:
            while not self.consumer.processes_completed():

                if prog == self.consumer.total_length:
                    print('program should end')
                    print(f' total_processes={self.consumer.total_processes} completed processes= {self.consumer.completed_processes}')
                    state_ids = [actor.get_state.remote() for _, actor in self.consumer.datasets.items()]
                    ray.wait(state_ids, timeout=30.0)
                    states = ray.get(state_ids)
                    a = 0

                no_of_samples = self.consumer.consume()
                if self.consumer.processes_completed():
                    continue
                self.consumer.transform()
                pbar.update(no_of_samples)
                prog += no_of_samples

        print('PIPELINE COMPLETE: Beginning shutdown process =>')
        self.consumer.spin_down_processes()
        use_context.performance_profile.close()

    def execute_downstream(self):
        x_train, x_test, y_train, y_test, features = self.provider.provide()

        # self.trainable.train(x_train, y_train)
        # self.trainable.evaluate(x_test, y_test)
        # self.trainable.diagnose()
        use_context.performance_profile.close()


if __name__ == "__main__":
    freeze_support()
    ray.init(log_to_driver=False)
    global_res = ray.available_resources()
    pipe = CB2Pipeline('../../configs/cb2/pipeline.config.yaml')
    pipe.execute_clean()
    ray.shutdown()
    a = 0
