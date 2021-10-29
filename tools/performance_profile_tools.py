from time import time
import tools.file_system as fs
import os
import tools.py_tools as pyt
from contextlib import contextmanager


class PerformanceProfile:

    def __init__(self, profile_path=None, filename=None, ):

        self.filename = filename if filename else fs.filename(frame_depth=2)
        self.profile_path = f'{profile_path}.profile.json' if profile_path else 'default.profile.json'
        self.dir_name = fs.path('../resources/performance_profile/')
        self.profile = {}

        self.block_name = None
        self.total_time = None
        self.start_time = None
        self.end_time = None
        self.read_profile()

    @contextmanager
    def __call__(self, block_name: str):
        self.block_name = block_name
        self.start_time = time()
        yield
        self.end_time = time()
        self.total_time = self.end_time - self.start_time
        self.update_profile()

    def read_profile(self):

        self.profile_path = fs.path(os.path.join(self.dir_name, self.profile_path))
        fs.make_dir(self.dir_name)

        if not fs.path_exists(self.profile_path):
            fs.touch(self.profile_path)
            return
        else:
            self.profile = fs.load_json(self.profile_path)

    def update_profile(self):
        self.profile = pyt.put(self.profile, self.total_time, [self.filename, self.block_name])

    def close(self):
        fs.save_json(self.profile_path, self.profile, optimise=False)



