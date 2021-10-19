from sklearn.model_selection import train_test_split
import numpy as np


class Provider:

    def __init__(self, config: dict):
        self.config = config

    def __call__(self, datasets) -> tuple:

        dataset_name = self.config['dataset_name']
        y_names = self.config['y_names']
        y_target = self.config['y_target']
        test_size = self.config['test_size']
        shuffle = self.config['shuffle']
        stratify = self.config['stratify']
        random_seed = self.config['random_seed']

        data = datasets[dataset_name]['data']
        feature_names = data.columns.tolist()

        y = data[y_target].to_frame(name=y_target)
        x = data.drop([y_names], axis=1)



