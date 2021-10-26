from sklearn.model_selection import train_test_split
import numpy as np
import tools.provider_tools as pt


class Provider:

    def __init__(self, config: dict):
        self.config = config

    def provide(self) -> tuple:

        dataset_dir = self.config['dataset_dir']
        preview_frac = self.config['preview_frac']
        y_names = self.config['y_names']
        y_target = self.config['y_target']
        test_size = self.config['test_size']
        shuffle = self.config['shuffle']
        stratify = self.config['stratify']
        random_seed = self.config['random_seed']

        # TODO Load in data
        virtual_db = pt.VirtualDb(dataset_dir)
        virtual_db.anchor()
        database = virtual_db.view(view_all=True, merge_partitions=True)
        a = 0







