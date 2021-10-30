from time import time
import tools.file_system as fs
import os
import tools.py_tools as pyt
from contextlib import contextmanager
from collections import OrderedDict


class PerformanceProfile:

    def __init__(self, config: dict):

        self.profile_path = f'{config.get("name")}.profile.json' if config.get('name') else 'default.profile.json'
        self.overwrite_profile = config.get('overwrite_profile', True)
        self.dir_name = fs.path('../resources/performance_profile/')
        self.profile = OrderedDict()

        self.block_name = None
        self.total_time = None
        self.start_time = None
        self.end_time = None
        self.read_profile()

    def read_profile(self):

        self.profile_path = fs.path(os.path.join(self.dir_name, self.profile_path))
        fs.make_dir(self.dir_name)

        if self.overwrite_profile or not fs.path_exists(self.profile_path) or fs.is_file_empty(self.profile_path):
            fs.touch(self.profile_path, overwrite=self.overwrite_profile)
            return
        else:
            self.profile = fs.load_json(self.profile_path)
            # Reformat for processing
            for block_name, block in self.profile.items():
                for call_name, call_block in block.items():
                    self.profile[block_name][call_name] = {'calls': call_block, 'iteration': 0}

    @contextmanager
    def __call__(self, block_name=None, call_name=None, filename=None):
        filename = filename if filename else fs.filename(frame_depth=3)
        self.start_time = time()
        yield
        self.end_time = time()
        self.total_time = self.end_time - self.start_time
        self.update_profile(filename, block_name, call_name)

    def update_profile(self, filename, block_name, call_name=None):
        call_name = call_name if call_name else ''
        block = pyt.get(self.profile, [filename, block_name], {'iteration': 0, 'calls': {}})

        if block.get('iteration') is None:
            block['iteration'] = 0

        # If this this is the first time updating then reset everything for new recording.
        if block['iteration'] == 0:
            block['calls'] = {}

        block = pyt.put(block, self.total_time, ['calls', f'{call_name}_call_{len(block["calls"])}'])
        block['iteration'] += 1

        self.profile = pyt.put(self.profile, block, [filename, block_name])

    def close(self):
        # Reformat for better viewing in json format
        for block_name, block in self.profile.items():
            for call_name, call_block in block.items():
                self.profile[block_name][call_name] = call_block['calls']

        fs.save_json(self.profile_path, self.profile, optimise=False)



